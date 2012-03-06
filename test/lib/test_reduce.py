import types

from nose.tools import eq_
from nose.tools import ok_

from disco.core import Params

from inferno.lib.reduce import keyset_reduce


class TestKeysetReduce(object):

    def test_reduce(self):
        data = [
            ('["keyset_BBB", "key1", "key2"]', [1, 10]),
            ('["keyset_BBB", "key1", "key2"]', [2, 20]),
            ('["keyset_ZZZ", "key1", "key2"]', [1, 100]),
            ('["keyset_ZZZ", "key1", "key2"]', [2, 200]),
            ('["keyset_AAA", "key1", "key2"]', [1, 1000]),
            ('["keyset_AAA", "key1", "key2"]', [2, 2000])]
        expected = [
            (['keyset_BBB', 'key1', 'key2'], [3, 30]),
            (['keyset_ZZZ', 'key1', 'key2'], [3, 300]),
            (['keyset_AAA', 'key1', 'key2'], [3, 3000])]
        self._assert_reduce(data, expected)

    def test_reduce_assumes_input_keys_are_grouped_together(self):
        data = [
            ('["keyset_BBB", "key1", "key2"]', [1, 10]),
            ('["keyset_ZZZ", "key1", "key2"]', [1, 100]),
            ('["keyset_AAA", "key1", "key2"]', [1, 1000]),
            ('["keyset_BBB", "key1", "key2"]', [2, 20]),
            ('["keyset_ZZZ", "key1", "key2"]', [2, 200]),
            ('["keyset_AAA", "key1", "key2"]', [2, 2000])]
        expected = [
            (['keyset_BBB', 'key1', 'key2'], [1, 10]),
            (['keyset_ZZZ', 'key1', 'key2'], [1, 100]),
            (['keyset_AAA', 'key1', 'key2'], [1, 1000]),
            (['keyset_BBB', 'key1', 'key2'], [2, 20]),
            (['keyset_ZZZ', 'key1', 'key2'], [2, 200]),
            (['keyset_AAA', 'key1', 'key2'], [2, 2000])]
        # expected equals input b/c the input keys weren't grouped together
        self._assert_reduce(data, expected)

    def test_null_value(self):
        data = [
            ('["keyset", "key1", "key2"]', [1, None])]
        expected = [
            (['keyset', 'key1', 'key2'], [1, 0])]
        self._assert_reduce(data, expected)

    def test_float_value(self):
        data = [
            ('["keyset", "key1", "key2"]', [1, 1.234])]
        expected = [
            (['keyset', 'key1', 'key2'], [1, 1.234])]
        self._assert_reduce(data, expected)

    def test_long_value(self):
        data = [
            ('["keyset", "key1", "key2"]', [1, 1326664799000])]
        expected = [
            (['keyset', 'key1', 'key2'], [1, 1326664799000])]
        self._assert_reduce(data, expected)

    def test_string_value(self):
        data = [
            ('["keyset", "key1", "key2"]', [1, 'some_string'])]
        expected = [
            (['keyset', 'key1', 'key2'], [1, 0])]
        self._assert_reduce(data, expected)

    def test_should_throw_away_bad_data_and_continue(self):
        data = [
            ('["keyset", "key1", "key2"]', [1, 1]),
            ('key_not_json', [1, 2]),
            ('["keyset", "key1", "key2"]', [1, 3])]
        expected = [
            (['keyset', 'key1', 'key2'], [1, 1]),
            (['keyset', 'key1', 'key2'], [1, 3])]
        self._assert_reduce(data, expected)

    def _assert_reduce(self, data, expected):
        # turn disco_debug on for more code coverage
        params = Params(disco_debug=True)
        actual = keyset_reduce(data, params)
        ok_(isinstance(actual, types.GeneratorType))
        eq_(list(actual), expected)
