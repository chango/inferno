# -*- coding: utf-8 -*-

import cStringIO
import sys

from nose.tools import eq_

from disco.core import Params

from inferno.lib.result import keyset_result


class TestKeysetResult(object):

    def setUp(self):
        sys.stdout = self.capture_stdout = cStringIO.StringIO()
        self.params = Params()
        self.params.keysets = {
            'last_name_keyset': dict(
                key_parts=['_keyset', 'last_name'],
                value_parts=['count'],
             ),
            'first_name_keyset': dict(
                key_parts=['_keyset', 'first_name'],
                value_parts=['count'],
             )}

    def tearDown(self):
        sys.stdout = sys.__stdout__

    def test_utf8(self):
        data = [(['last_name_keyset', unicode('Términos', 'utf-8')], [30])]
        expected = ['last_name,count', 'Términos,30']
        self._assert_keyset_result(data, self.params, expected)

    def test_input_data_keyset_grouped_together(self):
        data = [
            (['first_name_keyset', 'Tim'], [100]),
            (['first_name_keyset', 'Tom'], [200]),
            (['first_name_keyset', 'Sam'], [300]),
            (['last_name_keyset', 'Clarke'], [10]),
            (['last_name_keyset', 'Harvey'], [20]),
            (['last_name_keyset', 'Martin'], [30])]
        expected = [
            'first_name,count',
            'Tim,100',
            'Tom,200',
            'Sam,300',
            'last_name,count',
            'Clarke,10',
            'Harvey,20',
            'Martin,30']
        self._assert_keyset_result(data, self.params, expected)

    def test_input_data_keyset_not_grouped_together(self):
        data = [
            (['first_name_keyset', 'Tim'], [100]),
            (['last_name_keyset', 'Clarke'], [10]),
            (['first_name_keyset', 'Tom'], [200]),
            (['last_name_keyset', 'Harvey'], [20]),
            (['first_name_keyset', 'Sam'], [300]),
            (['last_name_keyset', 'Martin'], [30])]
        expected = [
            'first_name,count',
            'Tim,100',
            'last_name,count',
            'Clarke,10',
            'first_name,count',
            'Tom,200',
            'last_name,count',
            'Harvey,20',
            'first_name,count',
            'Sam,300',
            'last_name,count',
            'Martin,30']
        self._assert_keyset_result(data, self.params, expected)

    def _assert_keyset_result(self, data, params, expected):
        keyset_result(data, params)
        eq_(self.capture_stdout.getvalue(), '\r\n'.join(expected) + '\r\n')
