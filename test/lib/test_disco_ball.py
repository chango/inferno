import json
import os
import tempfile

from mock import Mock
from mock import patch
from nose.tools import eq_

import disco.core
import tornado.testing
import tornado.web

from inferno.lib.daemon import InfernoDaemon
from inferno.lib.disco_ball import DiscoBall
from inferno.lib.http_server import MAPPINGS
from inferno.lib.job_factory import JobFactory


FAKE_JOB_ID = 'automatic_rule_1@532:4f2b9:f0d86'


class DiscoBallHandlerTestCase(tornado.testing.AsyncHTTPTestCase):

    def get_app(self):
        self.application = tornado.web.Application(MAPPINGS)
        self.application.disco_ball = DiscoBall(FakeInfernoDaemon(), None)
        self.application.unit_testing = True
        return self.application

    def get_path(self, port):
        return 'http://localhost:%s' % (port)

    def put_a_fake_job_in_the_history(self, path):
        url = '/rules/automatic/automatic_rule_1.json'
        expected = {'job_url': '%s/jobs/%s.json' % (path, FAKE_JOB_ID)}
        self.assert_json_response_to_post(url, expected)

    def assert_csv_response(self, url, expected, code=200):
        response = self.fetch(url)
        content_type = 'text/csv; charset=UTF-8'
        eq_(response.headers['Content-Type'], content_type)
        eq_(response.body, expected)
        eq_(response.code, code)

    def assert_json_response_to_post(self, url, expected, code=200):
        response = self.fetch(url, method='POST', body='')
        self._assert_json_response(expected, code, response)

    def assert_json_response(self, url, expected, code=200):
        response = self.fetch(url)
        self._assert_json_response(expected, code, response)

    def _assert_json_response(self, expected, code, response):
        content_type = 'application/json; charset=UTF-8'
        eq_(response.headers['Content-Type'], content_type)
        eq_(json.loads(response.body), expected)
        eq_(response.code, code)


class TestMainHandler(DiscoBallHandlerTestCase):

    def test_get(self):
        path = self.get_path(self.get_http_port())
        url = '/index.json'
        expected = {
            'jobs': '%s/jobs.json' % path,
            'rules': '%s/rules.json' % path}
        self.assert_json_response(url, expected)


class TestRuleHandler(DiscoBallHandlerTestCase):

    def test_post(self):
        path = self.get_path(self.get_http_port())
        url = '/rules/automatic/automatic_rule_1.json'
        expected = {'job_url': '%s/jobs/%s.json' % (path, FAKE_JOB_ID)}
        self.assert_json_response_to_post(url, expected)

    def test_post_unknown_rule(self):
        url = '/rules/automatic/unknown.json'
        expected = {
            'message': 'could not start job for rule: automatic.unknown',
            'code': 500}
        self.assert_json_response_to_post(url, expected, code=500)

    def test_get(self):
        url = '/rules/automatic/automatic_rule_1.json'
        expected = {
            'name': 'automatic_rule_1',
            'run': True,
            'archive': False,
            'map_input_stream': [
                'disco.worker.classic.func.task_input_stream',
                'disco.worker.classic.func.disco_input_stream',
                'inferno.lib.reader.csv_reader',
                'inferno.lib.reader.keyset_multiplier'],
            'map_function': 'inferno.lib.map.keyset_map',
            'reduce_function': 'inferno.lib.reduce.keyset_reduce',
            'parts_preprocess': [],
            'parts_postprocess': [],
            'keysets': {
                'keyset_1': {
                    'key_parts': ['_keyset', 'key_1'],
                    'table': None,
                    'value_parts': ['value_1'],
                    'column_mappings': None
                },
                'keyset_2': {
                    'key_parts': ['_keyset', 'key_2'],
                    'value_parts': ['value_2'],
                    'table': None,
                    'column_mappings': None
                },
            },
        }
        self.assert_json_response(url, expected)

    def test_get_unknown_rule(self):
        url = '/rules/automatic/unknown.json'
        expected = {'message': 'could not find rule', 'code': 500}
        self.assert_json_response(url, expected, code=500)


class TestModuleHandler(DiscoBallHandlerTestCase):

    def test_get(self):
        path = self.get_path(self.get_http_port())
        url = '/rules/automatic.json'
        expected = {
            'automatic': [
                '%s/rules/automatic/automatic_rule_1.json' % path,
                '%s/rules/automatic/automatic_rule_2.json' % path,
                '%s/rules/automatic/automatic_rule_3.json' % path]}
        self.assert_json_response(url, expected)

    def test_get_unknown_module(self):
        url = '/rules/unknown.json'
        expected = {'message': 'could not find rules', 'code': 500}
        self.assert_json_response(url, expected, code=500)


class TestRuleListHandler(DiscoBallHandlerTestCase):

    def test_get(self):
        path = self.get_path(self.get_http_port())
        url = '/rules.json'
        expected = {
            'automatic': [
                '%s/rules/automatic/automatic_rule_1.json' % path,
                '%s/rules/automatic/automatic_rule_2.json' % path,
                '%s/rules/automatic/automatic_rule_3.json' % path],
            'more_automatic': [
                '%s/rules/more_automatic/automatic_rule_4.json' % path],
            'manual': [
                '%s/rules/manual/manual_rule_1.json' % path,
                '%s/rules/manual/manual_rule_2.json' % path,
                '%s/rules/manual/manual_rule_3.json' % path],
            'more_manual': [
                '%s/rules/more_manual/manual_rule_4.json' % path]}
        self.assert_json_response(url, expected)


class TestJobHandler(DiscoBallHandlerTestCase):

    def test_get_job_list(self):
        path = self.get_path(self.get_http_port())
        self.put_a_fake_job_in_the_history(path)
        url = '/jobs.json'
        expected = {
            'jobs': [{
                'url': '%s/jobs/%s.json' % (path, FAKE_JOB_ID),
                'status': 'job.start',
                'id': FAKE_JOB_ID}]}
        self.assert_json_response(url, expected)

    def test_get_empty_job_list(self):
        url = '/jobs.json'
        expected = {'jobs': []}
        self.assert_json_response(url, expected)

    def test_get_specific_job(self):
        path = self.get_path(self.get_http_port())
        self.put_a_fake_job_in_the_history(path)
        url = '/jobs/%s.json' % FAKE_JOB_ID
        settings = self.application.disco_ball.instance.settings
        expected = {
            'settings': settings,
            'child_data': None,
            'current_stage': 'job.start',
            'rule_name': 'automatic.automatic_rule_1',
            'job_name': 'automatic_rule_1@532:4f2b9:f0d86',
            'rule': '%s/rules/automatic/automatic_rule_1.json' % path,
            'results': '%s/jobs/%s/results.csv' % (path, FAKE_JOB_ID),
        }
        self.assert_json_response(url, expected)


class TestJobResultHandler(DiscoBallHandlerTestCase):

    @patch.object(disco.core.Disco, 'jobinfo')
    @patch('inferno.lib.disco_ext.sorted_iterator')
    def test_get(self, mock_sorted_iterator, mock_jobinfo):
        mock_jobinfo.return_value = {'results': ['url_1', 'url_2']}
        mock_sorted_iterator.return_value = [
            (['keyset_1', 'Tim'], [100]),
            (['keyset_1', 'Tom'], [200]),
            (['keyset_1', 'Sam'], [300])]
        path = self.get_path(self.get_http_port())
        self.put_a_fake_job_in_the_history(path)
        url = '/jobs/%s/results.csv' % FAKE_JOB_ID
        expected = 'Tim,100\r\nTom,200\r\nSam,300\r\n'
        self.assert_csv_response(url, expected)

    @patch.object(disco.core.Disco, 'jobinfo')
    def test_get_empty_results(self, mock_jobinfo):
        mock_jobinfo.return_value = {'results': []}
        path = self.get_path(self.get_http_port())
        self.put_a_fake_job_in_the_history(path)
        url = '/jobs/%s/results.csv' % FAKE_JOB_ID
        expected = ''
        self.assert_csv_response(url, expected)


class FakeInfernoDaemon(InfernoDaemon):

    def __init__(self):
        here = os.path.dirname(__file__)
        rules_dir = os.path.join(here, '..', 'fixture', 'fake_rules')
        settings = {
            'rules_directory': rules_dir,
            'pid_dir': tempfile.gettempdir()}
        InfernoDaemon.__init__(self, settings)

    def run_rule(self, rule, *args, **kwargs):
        # just override and fake it
        job = JobFactory.create_job(rule, self.settings)
        job.job = Mock()
        job.job.name = FAKE_JOB_ID
        self.history[FAKE_JOB_ID] = job.job_msg
        return job.job_msg
