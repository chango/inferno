import collections


class peekable(object):
    """
    THIS CODE IS FROM THE PYTHON COOKBOOK.
    """

    def __init__(self, iterable):
        self._iterable = iter(iterable)
        self._cache = collections.deque()

    def __iter__(self):
        return self

    def _fillcache(self, n):
        if n is None:
            n = 1
        while len(self._cache) < n:
            self._cache.append(self._iterable.next())

    def next(self, n=None):
        self._fillcache(n)
        if n is None:
            result = self._cache.popleft()
        else:
            result = [self._cache.popleft() for i in range(n)]
        return result

    def peek(self, n=None):
        self._fillcache(n)
        if n is None:
            result = self._cache[0]
        else:
            result = [self._cache[i] for i in range(n)]
        return result
