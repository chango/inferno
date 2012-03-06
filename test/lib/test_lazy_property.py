import random

from nose.tools import eq_

from inferno.lib.lazy_property import lazy_property


class TestLazyProperty(object):

    @lazy_property
    def not_so_random(self):
        return random.randint(0, 100)

    def test_lazy_property(self):
        value1 = self.not_so_random
        value2 = self.not_so_random
        eq_(value1, value2)
