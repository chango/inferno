import logging
import os
import threading

from Queue import Empty
from multiprocessing import Queue
from multiprocessing import Process

import inferno.lib.daemon

from inferno.lib.http_server import launch_server
from inferno.lib.job_factory import JobFactory


log = logging.getLogger(__name__)


class DiscoBall(threading.Thread):

    def __init__(self, instance, port, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.instance = instance
        self.port = port
        cwd = os.path.abspath(os.path.dirname(__file__))
        self.base_path = os.path.abspath("%s/.." % cwd)
        self.stopped = False

    def spin(self):
        self.start()

    def jobinfo(self, job_id):
        settings = self.instance.settings
        disco, _ = JobFactory.get_disco_ddfs(settings)
        return disco.jobinfo(job_id)

    # The following methods are RPC style that get invoked through the pipe
    # interface to the Tornado process.

    def get_jobinfo(self, job_id):
        return self.jobinfo(job_id)

    def set_history(self, job_id, job):
        self.instance.history[job_id] = job

    def get_modules(self):
        mods = {}
        for mod, rules in self.instance.rules.iteritems():
            mods[mod] = [rule.name for rule in rules]
        return mods

    def get_rules(self, module_name):
        rule_names = []
        rules = self.instance.rules.get(module_name)
        if rules:
            rule_names = [rule.name for rule in rules]
        return rule_names

    def get_rule_summary(self, module_name, rule_name):
        summary = {}
        rule = self.instance.get_rule_named(module_name, rule_name)
        if rule:
            summary = rule.summary_dict()
        return summary

    def run_job(self, module_name, rule_name, params):
        rule = self.instance.get_rule_named(module_name, rule_name)
        if rule:
            return self.instance.run_rule(rule, False, params, True)
        else:
            return None

    def get_job_histories(self):
        return self.instance.history.values()

    def get_job_history(self, job_id):
        return self.instance.history.get(job_id)

    def run(self, *args, **kwargs):
        try:
            log.info('starting the disco ball....')
            from_parent, from_child = Queue(), Queue()
            self.server = Process(target=launch_server,
                    args=(self.base_path, self.port, from_parent, from_child))
            self.server.daemon = True
            self.server.start()

            while not self.stopped:
                msg = args = None
                try:
                    msg, args = from_child.get(True, 2)
                except Empty:
                    continue
                except Exception as err:
                    error_msg = "Error receiving message from Tornado: %s"
                    log.error(error_msg, err)

                if not msg:
                    continue

                try:
                    result = getattr(self, msg)(*args)
                except Exception as er:
                    error_msg = "Error executing RPC for message: %s. %s"
                    log.error(error_msg, msg, er)
                    from_parent.put(None)
                else:
                    try:
                        from_parent.put(result)
                    except Exception as e:
                        error_msg = "Error executing service %s. %s"
                        log.error(error_msg, msg, e)
                        from_parent.put(None)

        except Exception as e:
            error_msg = "Unable to start DiscoBall on port %s for job %s. %s"
            log.error(error_msg, self.port, self.name, e)
