import tempfile

from datetime import date

from nose.tools import eq_

from inferno.bin.run import _get_options
from inferno.bin.run import _get_settings


class TestOptions(object):

    def test_defaults(self):
        options = _get_options([])
        expected = {
            'data_file': None,
            'parameters': [],
            'parameter_file': None,
            'force': False,
            'profile': False,
            'debug': False,
            'disco_debug': False,
            'server': None,
            'settings_file': None,
            'source_tags': None,
            'result_tag': None,
            'day_start': None,
            'day_range': None,
            'day_offset': None,
            'immediate_rule': None,
            'rules_directory': None,
            'start_paused': False,
            'example_rules': None}
        eq_(options, expected)

    def test_force(self):
        self._assert_force('-f')
        self._assert_force('--force')

    def test_profile(self):
        self._assert_profile('-p')
        self._assert_profile('--profile')

    def test_debug(self):
        self._assert_debug('-d')
        self._assert_debug('--debug')

    def test_disco_debug(self):
        self._assert_disco_debug('-D')
        self._assert_disco_debug('--disco-debug')

    def test_server(self):
        self._assert_server('-s')
        self._assert_server('--server')

    def test_settings(self):
        self._assert_settings('-e')
        self._assert_settings('--settings')

    def test_rules_directory(self):
        self._assert_rules_directory('-y')
        self._assert_rules_directory('--rules-directory')

    def test_day_range(self):
        self._assert_day_range('-R')
        self._assert_day_range('--day-range')

    def test_day_offset(self):
        self._assert_day_offset('-O')
        self._assert_day_offset('--day-offset')

    def test_day_start(self):
        self._assert_day_start('-S')
        self._assert_day_start('--day-start')

    def test_source_tags(self):
        self._assert_source_tags('-t')
        self._assert_source_tags('--source-tags')

    def test_result_tag(self):
        self._assert_result_tag('-r')
        self._assert_result_tag('--result-tag')

    def test_immediate_rule(self):
        self._assert_immediate_rule('-i')
        self._assert_immediate_rule('--immediate-rule')

    def test_parameters(self):
        self._assert_parameters('-P')
        self._assert_parameters('--parameters')

    def _assert_force(self, flag):
        options = _get_options([flag])
        eq_(options['force'], True)

    def _assert_profile(self, flag):
        options = _get_options([flag])
        eq_(options['profile'], True)

    def _assert_debug(self, flag):
        options = _get_options([flag])
        eq_(options['debug'], True)

    def _assert_disco_debug(self, flag):
        options = _get_options([flag])
        eq_(options['disco_debug'], True)

    def _assert_settings(self, flag):
        options = _get_options([flag, 'path_to_settings_file'])
        eq_(options['settings_file'], 'path_to_settings_file')

    def _assert_rules_directory(self, flag):
        options = _get_options([flag, 'path_to_rules_directory'])
        eq_(options['rules_directory'], 'path_to_rules_directory')

    def _assert_day_range(self, flag):
        options = _get_options([flag, '10'])
        eq_(options['day_range'], 10)

    def _assert_day_offset(self, flag):
        options = _get_options([flag, '5'])
        eq_(options['day_offset'], 5)

    def _assert_day_start(self, flag):
        options = _get_options([flag, '2012-12-01'])
        eq_(options['day_start'], date(2012, 12, 1))

    def _assert_immediate_rule(self, flag):
        options = _get_options([flag, 'some_module.some_rule'])
        eq_(options['immediate_rule'], 'some_module.some_rule')

    def _assert_result_tag(self, flag):
        options = _get_options([flag, 'some_result_tag'])
        eq_(options['result_tag'], 'some_result_tag')

    def _assert_parameters(self, flag):
        # one param
        options = _get_options([flag, 'some_param: some_value'])
        eq_(options['some_param'], 'some_value')

        # many params
        options = _get_options([
            flag, 'some_param_1: some_value_1',
            flag, 'some_param_2: some_value_2'])
        eq_(options['some_param_1'], 'some_value_1')
        eq_(options['some_param_2'], 'some_value_2')

        # integer
        options = _get_options([flag, 'some_int: 100'])
        eq_(options['some_int'], 100)

        # list of integers
        options = _get_options([flag, 'some_int_list: [100, 200, 300,]'])
        eq_(options['some_int_list'], [100, 200, 300])

    def _assert_source_tags(self, flag):
        # one tag
        options = _get_options([flag, 'tag1'])
        eq_(options['source_tags'], ['tag1'])

        # many tags
        options = _get_options([flag, 'tag1,tag2'])
        eq_(options['source_tags'], ['tag1', 'tag2'])

    def _assert_server(self, flag):
        options = _get_options([flag, 'some_server'])
        eq_(options['server'], 'some_server')


class TestSettings(object):

    def test_get_settings(self):
        # settings default
        options = _get_options(['-e', 'some_unknown_settings_file'])
        settings = _get_settings(options)
        eq_(settings['server'], 'localhost')

        # options override
        options = _get_options(['-s', 'some_server'])
        settings = _get_settings(options)
        eq_(settings['server'], 'some_server')

        # file override
        settings_file = self._create_settings_file()
        options = _get_options(['-e', settings_file])
        settings = _get_settings(options)
        eq_(settings['server'], 'another_server')

        # server from -s trumps -e
        settings_file = self._create_settings_file()
        options = _get_options(['-e', settings_file, '-s', 'some_server'])
        settings = _get_settings(options)
        eq_(settings['server'], 'some_server')

    def _create_settings_file(self):
        (_, settings_file) = tempfile.mkstemp()
        with open(settings_file, 'w') as f:
            f.write("server: another_server\n")
        return settings_file
