from nose.tools import assert_raises
from nose.tools import eq_

from inferno.lib.peekable import peekable


class TestPeekable(object):

    def test_get_one(self):
        p = peekable(range(4))
        eq_(p.peek(), 0)
        eq_(p.next(), 0)

    def test_get_list(self):
        p = peekable(range(4))
        eq_(p.peek(1), [0])
        eq_(p.next(1), [0])

    def test_look_ahead_then_grab_some(self):
        p = peekable(range(4))
        eq_(p.peek(3), [0, 1, 2])
        eq_(p.next(2), [0, 1])

    def test_walking_off_the_end(self):
        p = peekable(range(100))
        assert_raises(StopIteration, p.peek, 101)
        eq_(p.peek(1), [0])
        assert_raises(StopIteration, p.next, 101)
        eq_(p.next(1), [0])

    def test_iter(self):
        actual = [x for x in peekable(range(4))]
        eq_(actual, [0, 1, 2, 3])
