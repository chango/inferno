import csv
import os
import StringIO
import types

from nose.tools import eq_
from nose.tools import ok_
from nose.tools import raises

from disco.core import Params

from inferno.lib.reader import csv_reader
from inferno.lib.reader import json_reader
from inferno.lib.reader import keyset_multiplier


class TestKeysetMultiplier(object):

    def test_keyset_multiplier(self):
        params = Params()
        params.keysets = {
            'last_name_keyset': dict(
                key_parts=['_keyset', 'last_name'],
                value_parts=['count'],
             ),
            'first_name_keyset': dict(
                key_parts=['_keyset', 'first_name'],
                value_parts=['count'],
             )}
        data = [
            {'first_name': 'Willow', 'last_name': 'Harvey'},
            {'first_name': 'Noam', 'last_name': 'Clarke'}]
        expected = [
            {
                'first_name': 'Willow',
                'last_name': 'Harvey',
                '_keyset': 'first_name_keyset'
            },
            {
                'first_name': 'Willow',
                'last_name': 'Harvey',
                '_keyset': 'last_name_keyset'
            },
            {
                'first_name': 'Noam',
                'last_name': 'Clarke',
                '_keyset': 'first_name_keyset'
            },
            {
                'first_name': 'Noam',
                'last_name': 'Clarke',
                '_keyset': 'last_name_keyset'
            }]
        actual = keyset_multiplier(data, None, None, params)
        ok_(isinstance(actual, types.GeneratorType))
        eq_(list(actual), expected)


class TestJsonReader(object):

    def test_json_reader(self):
        self._assert_json_reader('input.json')

    def test_json_with_empty_lines(self):
        self._assert_json_reader('input_empty_lines.json')

    def test_json_with_bad_lines(self):
        self._assert_json_reader('input_bad_lines.json')

    @raises(Exception)
    def test_bad_stream(self):
        list(json_reader(10000, None, None, None))

    def _assert_json_reader(self, name):
        here = os.path.dirname(__file__)
        path = os.path.join(here, '..', 'fixture', name)
        actual = json_reader(open(path), None, None, None)
        expected = [
            {
                'timestamp': 1319475603759L,
                'token': '92f80832-f43b-11e0-a45f-00259035c924',
                'is_new': False,
                'id': 10000
            },
            {
                'timestamp': 1319475603987L,
                'token': '9e28ace0-fe61-11e0-97da-00259006bc9c',
                'is_new': True,
                'id': 20000
            }
        ]
        ok_(isinstance(actual, types.GeneratorType))
        eq_(list(actual), expected)


class TestCSVReader(object):

    def test_csv_reader(self):
        fields = ('field_1', 'field_2', 'field_3')
        values = 'value_1\tvalue_2\tvalue_3'
        expected = [{
            'field_1': 'value_1',
            'field_2': 'value_2',
            'field_3': 'value_3'}]
        self._assert_csv_reader(fields, values, expected)

    def test_more_fields_than_input(self):
        fields = ('field_1', 'field_2', 'field_3')
        values = 'value_1\tvalue_2'
        expected = [{
            'field_1': 'value_1',
            'field_2': 'value_2',
            'field_3': None}]
        self._assert_csv_reader(fields, values, expected)

    def test_less_fields_than_input(self):
        fields = ('field_1', 'field_2', 'field_3')
        values = 'value_1\tvalue_2\tvalue_3\tvalue_4'
        expected = [{
            'field_1': 'value_1',
            'field_2': 'value_2',
            'field_3': 'value_3'}]
        self._assert_csv_reader(fields, values, expected)

    def test_empty_value_in_the_middle(self):
        fields = ('field_1', 'field_2', 'field_3')
        values = 'value_1\t\tvalue_3'
        expected = [{
            'field_1': 'value_1',
            'field_2': '',
            'field_3': 'value_3'}]
        self._assert_csv_reader(fields, values, expected)

    def test_empty_values_on_each_side(self):
        fields = ('field_1', 'field_2', 'field_3')
        values = '\tvalue_2\t'
        expected = [{
            'field_1': '',
            'field_2': 'value_2',
            'field_3': ''}]
        self._assert_csv_reader(fields, values, expected)

    def test_no_fields(self):
        fields = None
        values = 'value_1\tvalue_2\tvalue_3'
        expected = [{
            '0': 'value_1',
            '1': 'value_2',
            '2': 'value_3'}]
        self._assert_csv_reader(fields, values, expected)

    def test_empty_line(self):
        fields = ('field_1', 'field_2', 'field_3')
        values = '\n'
        expected = []
        self._assert_csv_reader(fields, values, expected)

    def _assert_csv_reader(self, fields, values, expected):
        stream = StringIO.StringIO(values)
        params = Params()
        params.csv_fields = fields
        params.csv_dialect = csv.excel_tab
        actual = csv_reader(stream, None, None, params)
        ok_(isinstance(actual, types.GeneratorType))
        eq_(list(actual), expected)
