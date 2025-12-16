import argparse
import random
import time
import math
import statistics
import uuid
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import spanner


@dataclass
class BenchmarkConfig:
    project_id: str
    instance_id: str
    database_id: str
    table: str
    key_column: str
    payload_column: str
    workers: int
    duration: float
    workload: str
    read_ratio: float
    initial_rows: int
    payload_size: int
    pool_size: int


class WorkerStats:
    def __init__(self) -> None:
        self.read_latencies = []  # seconds
        self.write_latencies = []  # seconds
        self.read_ops = 0
        self.write_ops = 0
        self.errors = 0


def parse_args() -> BenchmarkConfig:
    parser = argparse.ArgumentParser(
        description="Simple Cloud Spanner benchmark (point reads + inserts)."
    )
    parser.add_argument("--project-id", required=True, help="GCP project id.")
    parser.add_argument("--instance-id", required=True, help="Spanner instance id.")
    parser.add_argument("--database-id", required=True, help="Spanner database id.")
    parser.add_argument("--table", required=True, help="Target table name.")
    parser.add_argument(
        "--key-column",
        required=True,
        help="Primary key column (STRING or INT64 recommended).",
    )
    parser.add_argument(
        "--payload-column",
        required=True,
        help="Column used to store a payload string.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of concurrent worker threads.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Benchmark duration in seconds (does not include data preparation).",
    )
    parser.add_argument(
        "--workload",
        choices=["read", "write", "mixed"],
        default="mixed",
        help="Type of workload to run.",
    )
    parser.add_argument(
        "--read-ratio",
        type=float,
        default=0.5,
        help="Read ratio for mixed workload (0.0 - 1.0).",
    )
    parser.add_argument(
        "--initial-rows",
        type=int,
        default=10_000,
        help="Number of rows to pre-insert for read workload.",
    )
    parser.add_argument(
        "--payload-size",
        type=int,
        default=512,
        help="Payload size in bytes for inserted rows.",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=0,
        help="Fixed session pool size (0 = use default pool).",
    )
    args = parser.parse_args()

    read_ratio = min(max(args.read_ratio, 0.0), 1.0)

    return BenchmarkConfig(
        project_id=args.project_id,
        instance_id=args.instance_id,
        database_id=args.database_id,
        table=args.table,
        key_column=args.key_column,
        payload_column=args.payload_column,
        workers=args.workers,
        duration=args.duration,
        workload=args.workload,
        read_ratio=read_ratio,
        initial_rows=args.initial_rows,
        payload_size=args.payload_size,
        pool_size=args.pool_size,
    )


def build_database(config: BenchmarkConfig):
    client = spanner.Client(project=config.project_id)
    instance = client.instance(config.instance_id)

    if config.pool_size > 0:
        # Use a fixed-size session pool so concurrency is predictable.
        try:
            pool = spanner.FixedSizePool(size=config.pool_size)
            database = instance.database(config.database_id, pool=pool)
        except AttributeError:
            print(
                "FixedSizePool not available in this google-cloud-spanner version; "
                "falling back to default pool.",
                flush=True,
            )
            database = instance.database(config.database_id)
    else:
        # Default BurstyPool, which auto-scales sessions.
        database = instance.database(config.database_id)

    return database


def prepare_dataset(database, config: BenchmarkConfig, payload: str):
    """
    Pre-populate the benchmark table with random keys so that
    point reads have something to hit.

    WARNING: This will insert `initial_rows` rows into the target table.
    Clean them up manually afterwards if needed.
    """
    if config.initial_rows <= 0:
        return []

    print(f"Preparing {config.initial_rows} rows for read workload ...", flush=True)
    keys = []
    batch_size = 100

    remaining = config.initial_rows
    while remaining > 0:
        this_batch = min(batch_size, remaining)
        values = []
        for _ in range(this_batch):
            key = str(uuid.uuid4()) # random.randint(1, 10000000)
            keys.append(key)
            values.append((key, payload))
        with database.batch() as batch:
            # print(f"Inserting batch of {values} rows...", flush=True)
            batch.insert(
                table=config.table,
                columns=(config.key_column, config.payload_column),
                values=values,
            )
        remaining -= this_batch

    print("Data preparation done.", flush=True)
    return keys


def do_read(database, config: BenchmarkConfig, key):
    # Single point read using the read API and a snapshot (strong by default).
    with database.snapshot() as snapshot:
        keyset = spanner.KeySet(keys=[[key]])
        results = snapshot.read(
            table=config.table,
            columns=(config.key_column, config.payload_column),
            keyset=keyset,
            limit=1,
        )
        # Force materialization so we actually measure server time.
        for _ in results:
            pass


def _write_unit_of_work(
    transaction, table: str, key_column: str, payload_column: str, key, payload: str
):
    # One row per transaction; Spanner will retry on aborts.
    transaction.insert_or_update(
        table=table,
        columns=(key_column, payload_column),
        values=[(key, payload)],
    )


def do_write(database, config: BenchmarkConfig, key, payload: str):
    database.run_in_transaction(
        _write_unit_of_work,
        config.table,
        config.key_column,
        config.payload_column,
        key,
        payload,
    )


def percentile(values, p):
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def worker_loop(
    worker_id: int,
    database,
    config: BenchmarkConfig,
    read_keys,
    payload: str,
    end_time: float,
) -> WorkerStats:
    stats = WorkerStats()
    rng = random.Random(worker_id ^ int(time.time()))

    if config.workload in ("read", "mixed") and not read_keys:
        raise RuntimeError(
            "Read workload requested but no read_keys available. "
            "Increase --initial-rows or switch to write-only workload."
        )

    while time.perf_counter() < end_time:
        if config.workload == "read":
            op_type = "read"
        elif config.workload == "write":
            op_type = "write"
        else:
            op_type = "read" if rng.random() < config.read_ratio else "write"

        start = time.perf_counter()
        try:
            if op_type == "read":
                key = rng.choice(read_keys)
                do_read(database, config, key)
                stats.read_ops += 1
                stats.read_latencies.append(time.perf_counter() - start)
            else:
                key = str(uuid.uuid4())
                do_write(database, config, key, payload)
                stats.write_ops += 1
                stats.write_latencies.append(time.perf_counter() - start)
        except Exception as exc:  # noqa: BLE001
            stats.errors += 1
            # Optional: log first few errors
            if stats.errors <= 3:
                print(f"[worker {worker_id}] error: {exc}", flush=True)

    return stats


def merge_stats(stats_list):
    total = WorkerStats()
    for s in stats_list:
        total.read_latencies.extend(s.read_latencies)
        total.write_latencies.extend(s.write_latencies)
        total.read_ops += s.read_ops
        total.write_ops += s.write_ops
        total.errors += s.errors
    return total


def print_summary(config: BenchmarkConfig, elapsed: float, total: WorkerStats):
    total_ops = total.read_ops + total.write_ops
    print()
    print("=== Cloud Spanner benchmark results ===")
    print(
        f"Workload={config.workload} "
        f"(read_ratio={config.read_ratio:.2f} for mixed), "
        f"workers={config.workers}, duration={elapsed:.1f}s, pool_size={config.pool_size}, payload_size={config.payload_size}"
    )
    if elapsed > 0:
        print(
            f"Total ops={total_ops} "
            f"({total_ops / elapsed:.1f} ops/s), errors={total.errors}"
        )
    else:
        print(f"Total ops={total_ops}, errors={total.errors}")

    def summarize(label, count, latencies):
        if not latencies or count == 0:
            print(f"{label}: no operations")
            return
        lat_ms = [x * 1000.0 for x in latencies]
        print(f"{label}: {count} ops")
        print(f"  avg={statistics.mean(lat_ms):.2f} ms")
        print(f"  p50={percentile(lat_ms, 50):.2f} ms")
        print(f"  p95={percentile(lat_ms, 95):.2f} ms")
        print(f"  p99={percentile(lat_ms, 99):.2f} ms")

    summarize("Reads", total.read_ops, total.read_latencies)
    summarize("Writes", total.write_ops, total.write_latencies)


def main():
    config = parse_args()
    database = build_database(config)
    payload = "x" * config.payload_size

    read_keys = []
    if config.workload in ("read", "mixed"):
        read_keys = prepare_dataset(database, config, payload)

    print("Starting benchmark run ...", flush=True)
    start = time.perf_counter()
    end_time = start + config.duration

    stats_list = []
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = [
            executor.submit(
                worker_loop,
                i,
                database,
                config,
                read_keys,
                payload,
                end_time,
            )
            for i in range(config.workers)
        ]
        for f in as_completed(futures):
            stats_list.append(f.result())

    elapsed = time.perf_counter() - start
    total = merge_stats(stats_list)
    print_summary(config, elapsed, total)


if __name__ == "__main__":
    main()


# python .\projects\spanner_benchmark.py --project-id=project-58610c92-efde-4208-998 --instance-id=benchmark-2025 --database-id=cymbal --table=Benchmark --key-column=Id --payload-column=Payload --workers=8 --duration=120 --workload=mixed --read-ratio=0.7 --initial-rows=5000 --payload-size=1024 --pool-size=20


# === Cloud Spanner benchmark results ===
# Workload=mixed (read_ratio=0.70 for mixed), workers=8, duration=120.2s, pool_size=20
# Total ops=5737 (47.7 ops/s), errors=0
# Reads: 3989 ops
#   avg=128.28 ms
#   p50=126.16 ms
#   p95=130.70 ms
#   p99=217.18 ms
# Writes: 1748 ops
#   avg=256.96 ms
#   p50=252.87 ms
#   p95=260.39 ms
#   p99=351.96 ms


# === Cloud Spanner benchmark results ===
# Workload=mixed (read_ratio=0.70 for mixed), workers=200, duration=120.4s, pool_size=50
# Total ops=67398 (559.7 ops/s), errors=0
# Reads: 47278 ops
#   avg=276.28 ms
#   p50=267.51 ms
#   p95=317.73 ms
#   p99=382.12 ms
# Writes: 20120 ops
#   avg=545.15 ms
#   p50=534.00 ms
#   p95=616.33 ms
#   p99=670.06 ms



# === Cloud Spanner benchmark results ===
# Workload=mixed (read_ratio=0.70 for mixed), workers=500, duration=120.9s, pool_size=100
# Total ops=68823 (569.4 ops/s), errors=0
# Reads: 48342 ops
#   avg=677.01 ms
#   p50=668.39 ms
#   p95=757.70 ms
#   p99=788.26 ms
# Writes: 20481 ops
#   avg=1340.52 ms
#   p50=1337.58 ms
#   p95=1440.47 ms
#   p99=1480.05 ms





# ==========================================================================================

# Low load baseline

# Workload=mixed (read_ratio=0.80 for mixed), workers=8, duration=120.2s, pool_size=16, payload_size=512
# Total ops=6217 (51.7 ops/s), errors=0

# Reads: 4982 ops
#   avg=128.94 ms
#   p50=124.91 ms
#   p95=147.42 ms
#   p99=176.96 ms

# Writes: 1235 ops
#   avg=257.55 ms
#   p50=250.71 ms
#   p95=281.50 ms
#   p99=351.09 ms



# ==========================================================================================

# Moderate load baseline

# Workload=mixed (read_ratio=0.70 for mixed), workers=16, duration=300.2s, pool_size=32, payload_size=1024
# Total ops=28376 (94.5 ops/s), errors=0

# Reads: 19665 ops
#   avg=129.81 ms
#   p50=126.13 ms
#   p95=150.27 ms
#   p99=183.62 ms

# Writes: 8711 ops
#   avg=258.22 ms
#   p50=252.89 ms
#   p95=283.64 ms
#   p99=326.15 ms



# ==========================================================================================

# High load baseline

# Workload=mixed (read_ratio=0.70 for mixed), workers=64, duration=300.2s, pool_size=128, payload_size=1024
# Total ops=109484 (364.6 ops/s), errors=0

# Reads: 76416 ops
#   avg=135.53 ms
#   p50=130.01 ms
#   p95=161.64 ms
#   p99=221.54 ms

# Writes: 33068 ops
#   avg=267.62 ms
#   p50=259.11 ms
#   p95=310.64 ms
#   p99=371.97 ms



# ==========================================================================================

# Stress load baseline

# Workload=mixed (read_ratio=0.70 for mixed), workers=128, duration=300.3s, pool_size=256, payload_size=1024
# Total ops=169451 (564.3 ops/s), errors=0

# Reads: 118742 ops
#   avg=176.56 ms
#   p50=168.29 ms
#   p95=222.12 ms
#   p99=272.35 ms

# Writes: 50709 ops
#   avg=344.03 ms
#   p50=335.91 ms
#   p95=402.61 ms
#   p99=469.15 ms