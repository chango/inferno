import glob
import logging
import os
import time

from datetime import datetime
from datetime import timedelta


log = logging.getLogger(__name__)


class DaemonPid(object):

    def __init__(self, settings):
        self._settings = settings
        self._format = '%Y-%m-%d %H:%M:%S'

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
        path = self._last_run_path(job)
        if not os.path.exists(path):
            return True
        with open(path) as f:
            now = datetime.utcnow()
            delta = timedelta(**job.rule.time_delta)
            last_run = datetime.strptime(f.read().strip(), self._format)
            next_run = last_run + delta
            result = next_run < now
            if not result:
                log.debug('Skipping job: %s (last: %s, next: %s)',
                    job.rule_name, last_run.strftime(self._format),
                    next_run.strftime(self._format))
            return result

    def create_pid(self, job):
        path = self._get_pid_path(job)
        if os.path.exists(path):
            return False
        with open(path, 'w') as f:
            f.write(str(os.getpid()))
        return True

    def create_last_run(self, job):
        path = self._last_run_path(job)
        with open(path, 'w') as f:
            now = datetime.utcnow()
            f.write(now.strftime(self._format))

    def remove_pid(self, job):
        path = self._get_pid_path(job)
        os.unlink(path)

    def _get_pid_path(self, job):
        return os.path.join(self._pid_dir, '%s.pid' % job.rule_name)

    def _last_run_path(self, job):
        return os.path.join(self._pid_dir, '%s.last_run' % job.rule_name)

    @property
    def _pid_dir(self):
        path = self._settings['pid_dir']
        if not os.path.exists(path):
            os.mkdir(path)
        return path
