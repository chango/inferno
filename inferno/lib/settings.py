import os

import yaml


def guess_settings():
    for settings_file in (
        os.path.expanduser('~/.inferno'),
        '/etc/inferno/settings.yaml'):
        if os.path.exists(settings_file):
            return settings_file
    return ''


defaults = {
    'log_config': '/etc/inferno/log.ini',
    'settings_file': guess_settings(),
    'spawn_delay': 5,
    'max_workers': 8,
    'pid_dir': '/var/run/inferno',
    'server': 'localhost',
    'rules_directory': '/apps/project/project/rules',
    'extra_python_paths': [],
}


class InfernoSettings(dict):

    def __init__(self, *args, **kwargs):
        # load the defaults
        super(InfernoSettings, self).update(defaults)

        # override with the settings file
        path = kwargs.get('settings_file') or self['settings_file']
        if path and os.path.exists(path):
            self.update(yaml.load(open(path)))

        # final overrides
        super(InfernoSettings, self).__init__(*args, **kwargs)
