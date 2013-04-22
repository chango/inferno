"""
Based on code from Python Recipes
http://code.activestate.com/recipes/499379-groupbysorted/
Licensed under the PSF License (http://docs.python.org/2/license.html)"""

import collections


class peekable(object):
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
