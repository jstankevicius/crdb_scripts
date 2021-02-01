import heapq

class PriorityQueue:

    def __init__(self):
        self.heap = []
        heapq.heapify(self.heap)

    def head(self):
        return self.heap[0] if not self.empty() else None

    def push(self, e):
        heapq.heappush(self.heap, e)

    def pop(self):
        return heapq.heappop(self.heap)

    def size(self):
        return len(self.heap)

    def empty(self):
        return self.size() == 0
