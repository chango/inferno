import disco.core

from mock import patch
from nose.tools import eq_, ok_

from inferno.lib.job_factory import JobFactory
from inferno.lib.rule import InfernoRule
from inferno.lib.settings import InfernoSettings


class TestJobFactory(object):

    @patch.object(disco.core.Disco, 'jobinfo')
    def test_create(self, mock_jobinfo):
        mock_jobinfo.return_value = {'results': ['url_1', 'url_2']}
        rule = InfernoRule(
            name='some_rule_name',
            source_tags=['some_tag'],
            some_extra_param='some_extra_value',
            day_range=2,
            day_offset=4)
        settings = InfernoSettings(
            settings_file='some_unknown_settings_file',
            server='some_server',
            day_range=3,
            day_offset=5,
            debug=True,
            disco_debug=True)
        expected_settings = {
            # the passed in overrides
            'day_range': 3,
            'day_offset': 5,
            'debug': True,
            'disco_debug': True,
            'server': 'some_server',
            'settings_file': 'some_unknown_settings_file',
            # the normal defaults
            'rules_directory': '/apps/project/project/rules',
            'extra_python_paths': [],
            'pid_dir': '/var/run/inferno',
            'max_workers': 8,
            'log_config': '/etc/inferno/log.ini',
        }
        expected_params = {
            # extra rule kwargs, plus keysets
            'some_extra_param': 'some_extra_value',
            'keysets': {
                '_default': {
                    'column_mappings': [],
                    'table': None,
                    'value_parts': [],
                    'parts_postprocess': [],
                    'parts_preprocess':[],
                    'key_parts': ['_keyset']}},
            # the passed in overrides
            'day_range': 3,
            'day_offset': 5,
            'debug': True,
            'disco_debug': True,
            'server': 'some_server',
            'settings_file': 'some_unknown_settings_file',
            # the normal defaults
            'rules_directory': '/apps/project/project/rules',
            'extra_python_paths': [],
            'pid_dir': '/var/run/inferno',
            'max_workers': 8,
            'log_config': '/etc/inferno/log.ini',
        }
        job = JobFactory.create_job(rule, settings)

        # check disco/ddfs
        eq_(str(job.ddfs), 'DDFS master at disco://some_server')
        eq_(str(job.disco), 'Disco master at disco://some_server')

        # check job.rule
        eq_(job.rule.name, 'some_rule_name')
        eq_(job.rule.source_tags, ['some_tag'])
        eq_(job.rule.day_range, 2)
        eq_(job.rule.day_offset, 4)

        # check job.settings
        for key, value in expected_settings.iteritems():
            ok_(key in job.settings)
            eq_(value, job.settings[key])

        # check job.params
        params = vars(job.params)
        for key, value in expected_params.iteritems():
            ok_(key in params)
            eq_(value, params[key])
