import glob
import logging
import os
import time

from datetime import datetime
from inferno.lib.datefile import Datefile


log = logging.getLogger(__name__)


class DaemonPid(object):

    def __init__(self, settings):
        self._settings = settings

    @property
    def processes(self):
        files = glob.glob(os.path.join(self._pid_dir, '*.pid'))
        result = []
        for pid_file in files:
            with open(pid_file) as f:
                pid = f.read().strip()
                result.append({
                    'pid': pid,
                    'timestamp': time.ctime(os.path.getctime(pid_file)),
                    'name': os.path.basename(pid_file).replace('.pid', ''),
                })
        return result

    def should_run(self, job):
        last_run = Datefile(self._pid_dir, "%s.last_run" % job.rule_name)

        if not self._get_pid_path(job) and \
            last_run.is_older_than(job.rule.time_delta):
            return True
        else:
            log.debug('Skipping job: %s (last: %s)',
                job.rule_name, last_run)
        return False

    def create_last_run(self, job):
        Datefile(self._pid_dir, "%s.last_run" % job.rule_name, timestamp=datetime.utcnow())

    def create_pid(self, job):
        path = self._get_pid_path(job)
        if os.path.exists(path):
            return False
        with open(path, 'w') as f:
            f.write(str(os.getpid()))
        return True

    def remove_pid(self, job):
        path = self._get_pid_path(job)
        os.unlink(path)

    def _get_pid_path(self, job):
        return os.path.join(self._pid_dir, '%s.pid' % job.rule_name)

    @property
    def _pid_dir(self):
        path = self._settings['pid_dir']
        if not os.path.exists(path):
            os.mkdir(path)
        return path
