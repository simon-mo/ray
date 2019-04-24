from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import heapq
from collections import deque


class FIFOQueue:
    def __init__(self):
        self.q = deque([])

    def push(self, item):
        self.q.append(item)

    def pop(self):
        return self.q.popleft()

    def try_pop(self):
        if len(self.q) == 0:
            return None
        else:
            return self.pop()

    def __len__(self):
        return len(self.q)


class PriorityQueue:
    """A min-heap class wrapping heapq module."""

    def __init__(self):
        self.q = []

    def push(self, item):
        heapq.heappush(self.q, item)

    def pop(self):
        return heapq.heappop(self.q)

    def try_pop(self):
        if len(self.q) == 0:
            return None
        else:
            return self.pop()

    def __len__(self):
        return len(self.q)
