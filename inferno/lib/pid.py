import glob
import logging
import os
import time

from datetime import datetime
from inferno.lib.datefile import Datefile

log = logging.getLogger(__name__)


def processes(pid_dir):
    files = glob.glob(os.path.join(pid_dir, '*.pid'))
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


def should_run(pid_dir, rule):
    if not os.path.exists(get_pid_path(pid_dir, rule)):
        last_run = Datefile(pid_dir, "%s.last_run" % rule.name)
        if last_run.is_older_than(rule.time_delta):
            return True
        else:
            log.debug('Skipping job: %s (last: %s)',
                      rule.name, last_run)
    return False


def create_last_run(pid_dir, rule):
    Datefile(pid_dir, "%s.last_run" % rule.name,
             timestamp=datetime.utcnow())


def create_pid(pid_dir, rule, pid):
    path = get_pid_path(pid_dir, rule)
    try:
        with open(path, 'w') as f:
            f.write(pid)
        return True
    except Exception as ex:
        raise ex


def remove_pid(pid_dir, rule):
    path = get_pid_path(pid_dir, rule)
    os.unlink(path)


def get_pid_path(pid_dir, rule):
    return os.path.join(pid_dir, '%s.pid' % rule.name)


def pid_dir(settings):
    path = settings['pid_dir']
    if not os.path.exists(path):
        os.mkdir(path)
    return path