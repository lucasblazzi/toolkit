class Node:

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None


class LRUCache(object):

    def __init__(self, capacity):
        if capacity <= 0:
            raise TypeError("Capacity must be positive")
        self.capacity = capacity
        self.cache = dict()
        self.sequence = list()

        self.head = Node(0, 0)
        self.tail = Node(0, 0)
        self.tail.prev = self.head
        self.head.next = self.tail

    def _add_node_to_head(self, node):
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node
        return
    
    def _remove_node(self, node):
        previous_node = node.prev
        next_node = node.next
        previous_node.next = next_node
        next_node.prev = previous_node
        return

    def get(self, key):
        if key in self.cache:
            node = self.cache[key]
            self._remove_node(node)
            self._add_node_to_head(node)
            return node.value
        return -1

    def put(self, key, value):
        if key in self.cache:
            node = self.cache[key]
            node.value = value
            self._remove_node(node)
            self._add_node_to_head(node)
        else:
            new_node = Node(key, value)
            self.cache[key] = new_node
            self._add_node_to_head(new_node)
            
            if len(self.cache) > self.capacity:
                lru = self.tail.prev
                self._remove_node(lru)
                del self.cache[lru.key]
        


result = list()
cache = LRUCache(2)
result.append(cache.put(1, 1))
result.append(cache.put(2, 2))
result.append(cache.get(1))
result.append(cache.put(3, 3))
result.append(cache.get(2))
result.append(cache.put(4, 4))
result.append(cache.get(1))
result.append(cache.get(3))
result.append(cache.get(4))
print(result)
# Expected output: [null,null,null,1,null,-1,null,-1,3,4]

