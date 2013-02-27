import logging
import os
import pickle
import signal
import sys
import time

from multiprocessing import Queue
from multiprocessing.process import Process
from multiprocessing.reduction import reduce_connection
from threading import RLock

from setproctitle import setproctitle

from inferno.lib.disco_ball import DiscoBall
from inferno.lib.job_factory import JobFactory
from inferno.lib.lookup_rules import get_rule_dict
from inferno.lib.lookup_rules import get_rules
from inferno.lib.lookup_rules import get_rules_by_name
from inferno.lib import pid


log = logging.getLogger(__name__)


def pickle_connection(connection):
    return pickle.dumps(reduce_connection(connection))


def unpickle_connection(pickled_connection):
    (func, args) = pickle.loads(pickled_connection)
    return func(*args)


def run_rule_async(rule_name, settings, queue):
    setproctitle("inferno - %s" % rule_name)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    response_sent = False
    parent = 'http://127.0.0.1:%d' % settings.get('inferno_http_port', 6970)
    rules = get_rules_by_name(
        rule_name, settings['rules_directory'], immediate=False)
    if rules and len(rules) > 0:
        rule = rules[0]
    else:
        log.error('No rule exists with rule_name: %s' % rule_name)
        raise Exception('No rule exists with rule_name: %s' % rule_name)
    pid_dir = pid.pid_dir(settings)
    # updates pid inside rule_name.pid
    log.info("Running %s" % rule.name)
    try:
        pid.create_pid(pid_dir, rule, str(os.getpid()))
        job = JobFactory.create_job(rule, settings, parent)
        if job.start():
            queue.put({'job': job.job_msg})
            response_sent = True
            job.wait()
        else:
            queue.put({'info': "not enough blobs"})
            response_sent = True
    except Exception as e:
        if not response_sent:
            queue.put({'error': e.message})
        log.error('Error running job %s: %s',
                  rule_name, e, exc_info=sys.exc_info())
    finally:
        pid.remove_pid(pid_dir, rule)
        os._exit(0)


class InfernoDaemon(object):

    def __init__(self, settings):
        self.lock = RLock()
        self.settings = settings
        self._paused = settings.get('start_paused')
        self._stopped = False
        self._rules = get_rule_dict(settings['rules_directory'], True)
        self.history = {}

    @property
    def port(self):
        return self.settings.get('inferno_http_port', 6970)

    @property
    def rules(self):
        with self.lock:
            return self._rules

    @property
    def paused(self):
        with self.lock:
            return self._paused

    @property
    def stopped(self):
        with self.lock:
            return self._stopped

    def get_rule_named(self, mod, rule_name):
        with self.lock:
            for rule in self._rules.get(mod, []):
                if rule.name == rule_name:
                    return rule

    def run_rule(self, rule, params=None, wait_for_id=False):
        try:
            log.info('trying job %s' % rule.name)
            job_settings = self.settings.copy()
            if params:
                job_settings.update(params)
            name = rule.qualified_name
            queue = Queue()
            args = (name, job_settings, queue)
            Process(target=run_rule_async, args=args).start()
            if wait_for_id:
                msg = queue.get(True)
                if msg and 'job' in msg:
                    job = msg['job']
                    self.history[job['job_name']] = job
                    return msg['job']
                elif 'error' in msg:
                    log.error('Error creating job: %s' % msg)
                    return None
        except Exception as e:
            log.error("Error running rule: %s" % e)
            raise e

    def die(self, x=None, y=None):
        pid = os.getpid()
        if not self.disco_ball.stopped:
            log.info('dying... %d' % pid)
            try:
                self.disco_ball.stopped = True
                self.disco_ball.server.terminate()
            except:
                pass
            os._exit(0)
        else:
            log.info('dead... %d' % pid)

    def start(self):
        signal.signal(signal.SIGTERM, self.die)

        log.info('Starting Inferno...')
        auto_rules = get_rules(self.settings['rules_directory'])

        port = self.settings.get('inferno_http_port', 6970)
        self.disco_ball = DiscoBall(instance=self, port=port)
        self.disco_ball.spin()
        log.info('finished spinning ball')
        pid_dir = pid.pid_dir(self.settings)

        # keep cycling through the automatic rules
        while not self.stopped:
            # cycle through all the automatic rules
            for rule in auto_rules:
                if self.stopped:
                    break
                # skip this rule
                # check if pid file exists
                if not rule.run or self.paused or not pid.should_run(pid_dir, rule):
                    continue

                pid.create_pid(pid_dir, rule, 'N/A')
                pid.create_last_run(pid_dir, rule)
                self.run_rule(rule)
            time.sleep(1)
        self.die()
