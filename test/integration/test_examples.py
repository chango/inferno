# -*- coding: utf-8 -*-

import cStringIO
import os
import sys
import tempfile

from nose.tools import eq_

from inferno.lib.disco_ext import get_disco_handle
from inferno.lib.job_factory import JobFactory
from inferno.lib.settings import InfernoSettings


class TestExamples(object):

    json_data = """
{"first_name":"Joan", "last_name":"Términos"}
{"first_name":"Willow", "last_name":"Harvey"}
{"first_name":"Noam", "last_name":"Clarke"}
{"first_name":"Joan", "last_name":"Harvey"}
{"first_name":"Beatty", "last_name":"Clarke"}
"""
    csv_data = """
Joan,Términos
Willow,Harvey
Noam,Clarke
Joan,Harvey
Beatty,Clarke
"""

    def setUp(self):
        sys.stdout = self.capture_stdout = cStringIO.StringIO()
        here = os.path.dirname(__file__)
        self.rules_dir = os.path.join(here, '..', 'fixture', 'test_rules')
        self.tag = 'test:integration:chunk:users:2012-01-10'
        self.custom_tag_prefix = 'result:test:integration:last_names_result'
        self.result_tag_prefix = 'disco:job:results:last_names_result'

    def tearDown(self):
        sys.stdout = sys.__stdout__
        self.ddfs.delete(self.tag)
        for prefix in [self.custom_tag_prefix, self.result_tag_prefix]:
            for tag in self.ddfs.list(prefix):
                self.ddfs.delete(tag)

    def test_json(self):
        settings = InfernoSettings(
            rules_directory=self.rules_dir,
            immediate_rule='names.last_names_json')
        expected = [
            'last_name,count',
            'Clarke,2',
            'Harvey,2',
            'Términos,1']
        self._assert_integration_test(settings, expected, self.json_data)

    def test_csv(self):
        settings = InfernoSettings(
            rules_directory=self.rules_dir,
            immediate_rule='names.last_names_csv')
        expected = [
            'last_name,count',
            'Clarke,2',
            'Harvey,2',
            'Términos,1']
        self._assert_integration_test(settings, expected, self.csv_data)

    def test_many_keysets(self):
        settings = InfernoSettings(
            rules_directory=self.rules_dir,
            immediate_rule='names.first_and_last_names')
        expected = [
            'first_name,count',
            'Beatty,1',
            'Joan,2',
            'Noam,1',
            'Willow,1',
            'last_name,count',
            'Clarke,2',
            'Harvey,2',
            'Términos,1']
        self._assert_integration_test(settings, expected, self.json_data)

    def test_tag_results(self):
        settings = InfernoSettings(
            rules_directory=self.rules_dir,
            immediate_rule='names.last_names_result')
        expected = [
            'last_name,count',
            'Clarke,2',
            'Harvey,2',
            'Términos,1']
        self._assert_integration_test(settings, expected, self.json_data)
        tags = self.ddfs.list(self.custom_tag_prefix)
        eq_(len(tags), 1)
        tags = self.ddfs.list(self.result_tag_prefix)
        eq_(len(tags), 1)

    def _chunk_test_data(self, settings, data):
        (_, name) = tempfile.mkstemp()
        with open(name, 'w') as f:
            f.write(data)
        self.settings = settings
        (_, self.ddfs) = get_disco_handle(settings['server'])
        self.ddfs.delete(self.tag)
        self.ddfs.chunk(self.tag, [name])

    def _assert_integration_test(self, settings, expected, data):
        self._chunk_test_data(settings, data)
        JobFactory.execute_immediate_rule(settings)
        eq_(self.capture_stdout.getvalue(), '\r\n'.join(expected) + '\r\n')
