import json
import asyncio
from io import BytesIO
from copy import deepcopy
from datetime import datetime

import pandas as pd

from app.config import ENV
from app.modules.explorer import Explorer
from app.modules.excel import ExcelBuilder
from app.modules.metrics import CostMetrics


async def get_data(request):
    dc, sdc, smc, gmc, top10 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    for account in request["accounts"]:
        acc_req = deepcopy(request)
        acc_req["account"] = account
        explorer = Explorer(acc_req)

        dc = pd.concat([explorer.get_daily_costs(), dc])
        sdc = pd.concat([explorer.get_service_daily_costs(), sdc])
        smc = pd.concat([explorer.get_service_monthly_costs(), smc])
        gmc = pd.concat([explorer.get_generic_monthly_costs(), gmc])
        top10.extend(explorer.top10_recommendations)
        return {
            "resource_daily": dc,
            "service_daily": sdc,
            "service_monthly": smc,
            "generic_monthly": gmc,
            "top10_recommendations": top10
        }


async def handler(request):
    costs = await get_data(request)
    metrics = CostMetrics(**costs)
    template = json.load(open("costs/model.json"))
    buffer = ExcelBuilder(image).write_excel(template, request["name"])
    return data


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(handler())