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
    last_run = Datefile(pid_dir, "%s.last_run" % rule.name)
    if not os.path.exists(get_pid_path(pid_dir, rule)):
        if last_run.is_older_than(rule.time_delta):
            return True
        elif rule.retry:
            if os.path.exists(os.path.join(pid_dir, "%s.next_retry" % rule.name)):
                next_retry = Datefile(pid_dir, "%s.next_retry" % rule.name)
                now = datetime.utcnow()
                if now > datetime.strptime(str(next_retry), '%Y-%m-%d %H:%M:%S'):
                    return True
    log.debug('Skipping job: %s (last: %s)', rule.name, last_run)
    return False


def create_last_complete(pid_dir, rule):
    Datefile(pid_dir, "%s.last_complete" % rule.name, timestamp=datetime.utcnow())


def create_last_run(pid_dir, rule):
    Datefile(pid_dir, "%s.last_run" % rule.name, timestamp=datetime.utcnow())


def create_failed(pid_dir, rule):
    Datefile(pid_dir, "%s.failed" % rule.name, timestamp=datetime.utcnow())
    next_retry = os.path.join(pid_dir, "%s.next_retry" % rule.name)
    if os.path.exists(next_retry):
        os.unlink(next_retry)


def create_next_retry(pid_dir, rule):
    # we should retry this rule in rule.retry_delay number of hours
    from datetime import timedelta
    Datefile(pid_dir, "%s.next_retry" % rule.name,
             timestamp=datetime.utcnow() + timedelta(0, 0, 0, 0, 0, int(rule.retry_delay), 0))


def increment_retry_count(pid_dir, rule):
    path = os.path.join(pid_dir, "%s.retry_count" % rule.name)
    if os.path.exists(path):
        with open(path) as r:
            retry_count = int(r.readlines(1)[0]) + 1
        with open(path, 'w') as w:
            w.write(str(retry_count))
    else:
        with open(path, 'w') as w:
            w.write('1')


def get_retry_count(pid_dir, rule):
    path = os.path.join(pid_dir, "%s.retry_count" % rule.name)
    if os.path.exists(path):
        with open(path) as r:
            return int(r.readlines(1)[0])
    else:
        return 0


def create_pid(pid_dir, rule, pid):
    path = get_pid_path(pid_dir, rule)
    try:
        with open(path, 'w') as f:
            f.write(pid)
        return True
    except Exception as ex:
        raise ex


def clean_up(pid_dir, rule):
    retry_count = os.path.join(pid_dir, "%s.retry_count" % rule.name)
    next_retry = os.path.join(pid_dir, "%s.next_retry" % rule.name)
    failed = os.path.join(pid_dir, "%s.failed" % rule.name)
    if os.path.exists(retry_count):
        os.unlink(retry_count)
    if os.path.exists(next_retry):
        os.unlink(next_retry)
    if os.path.exists(failed):
        os.unlink(failed)


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
