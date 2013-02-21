import datetime
import types

from nose.tools import eq_
from nose.tools import ok_

from inferno.lib.map import keyset_map
from inferno.lib.rule import InfernoRule


class TestKeysetMap(object):

    def setUp(self):
        self.data = {
            'city': 'toronto',
            'country': 'canada',
            'population': 100,
            'size': 1000,
            'date': datetime.date(2012, 12, 01)}
        self.rule = InfernoRule(
            key_parts=['country', 'city'],
            value_parts=['population', 'size'])

    def test_keys_and_parts(self):
        expected = [('["_default","canada","toronto"]', [100, 1000])]
        self._assert_map(self.data, self.rule, expected)

    def test_missing_key_part_should_not_yield_result(self):
        del self.data['city']
        expected = []
        self._assert_map(self.data, self.rule, expected)

    def test_missing_value_part_should_yield_result(self):
        del self.data['size']
        expected = [('["_default","canada","toronto"]', [100, 0])]
        self._assert_map(self.data, self.rule, expected)

    def test_null_key_part_should_not_yield_result(self):
        self.data['city'] = None
        expected = []
        self._assert_map(self.data, self.rule, expected)

    def test_null_value_part_should_yield_result(self):
        self.data['size'] = None
        expected = [('["_default","canada","toronto"]', [100, None])]
        self._assert_map(self.data, self.rule, expected)

    def test_empty_key_part_should_yield_result(self):
        self.data['city'] = ''
        expected = [('["_default","canada",""]', [100, 1000])]
        self._assert_map(self.data, self.rule, expected)

    def test_empty_value_part_should_yield_result(self):
        self.data['size'] = ''
        expected = [('["_default","canada","toronto"]', [100, ''])]
        self._assert_map(self.data, self.rule, expected)

    def test_map_serialization(self):
        # key parts are str casted & json serialized, value parts are are not
        # (note the difference between the key date and value date results)
        rule = InfernoRule(
            key_parts=['date'],
            value_parts=['date'])
        expected = [('["_default","2012-12-01"]', [datetime.date(2012, 12, 1)])]
        self._assert_map(self.data, rule, expected)

    def test_field_transforms(self):
        def upper(val):
            return val.upper()

        rule = InfernoRule(
            key_parts=['country', 'city'],
            value_parts=['population', 'size'],
            field_transforms={'city': upper, 'country': upper})
        expected = [('["_default","CANADA","TORONTO"]', [100, 1000])]
        self._assert_map(self.data, rule, expected)

    def test_parts_preprocess_that_yields_multiple_parts(self):
        def lookup_language(parts, params):
            for language in ['french', 'english']:
                parts_copy = parts.copy()
                parts_copy['language'] = language
                yield parts_copy

        rule = InfernoRule(
            key_parts=['country'],
            value_parts=['language'],
            parts_preprocess=[lookup_language])
        expected = [
            ('["_default","canada"]', ['french']),
            ('["_default","canada"]', ['english'])]
        self._assert_map(self.data, rule, expected)

    def test_field_transforms_happen_after_parts_preprocess(self):
        def lookup_language(parts, params):
            for language in ['french', 'english']:
                parts_copy = parts.copy()
                parts_copy['language'] = language
                yield parts_copy

        def upper(val):
            return val.upper()

        rule = InfernoRule(
            key_parts=['country'],
            value_parts=['language'],
            parts_preprocess=[lookup_language],
            field_transforms={'language': upper})
        expected = [
            ('["_default","canada"]', ['FRENCH']),
            ('["_default","canada"]', ['ENGLISH'])]
        self._assert_map(self.data, rule, expected)

    def _assert_map(self, parts, rule, expected):
        # turn disco_debug on for more code coverage
        rule.params.disco_debug = True
        actual = keyset_map(parts, rule.params)
        ok_(isinstance(actual, types.GeneratorType))
        eq_(list(actual), expected)
