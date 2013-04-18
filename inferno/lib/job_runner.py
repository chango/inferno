import logging
from inferno.lib.disco_ext import get_disco_handle
from inferno.lib.job import InfernoJob
from inferno.lib.lookup_rules import get_rules_by_name
from inferno.lib.rule import (extract_subrules, deduplicate_rules,
                              flatten_rules)
from disco.error import JobError


log = logging.getLogger(__name__)


def _start_job(rule, settings, urls=None):
    job = InfernoJob(rule, settings, urls)
    return job.start()


def _run_concurrent_rules(rule_list, settings, urls_blackboard):
    def _get_rule_name(disco_job_name):
        return disco_job_name.rsplit('@')[0]

    jobs = []
    for rule in rule_list:
        urls = []
        for sub_rule in extract_subrules(rule):
            urls += urls_blackboard[sub_rule.name]
        job = _start_job(rule, settings, urls)
        if job:
            jobs.append(job)
        else:
            log.warn("Not enough blobs for %s" % rule.name)
            raise JobError('Not enough blobs for %s' % rule.name)

    job_results = {}
    stop = False
    server, _ = get_disco_handle(settings.get('server'))
    while jobs:
        inactive, active = server.results(jobs, 5000)
        for jobname, (status, results) in inactive:
            if status == "ready":
                job_results[_get_rule_name(jobname)] = results
            elif status == "dead":
                stop = True
        jobs = active
        if stop:
            break

    if stop:
        for jobname, _ in jobs:
            server.kill(jobname)
            raise JobError('One of the concurrent job failed.')
            # TODO purge automatically?

    return job_results


def _run_sequential_rules(rule_list, settings, urls_blackboard):
    for rule in rule_list:
        urls = []
        for sub_rule in extract_subrules(rule):
            urls += urls_blackboard[sub_rule.name]
        job = _start_job(rule, settings, urls)
        if job:
            job.wait()
        else:
            log.warn("Not enough blobs for %s" % rule.name)
            raise JobError('Not enough blobs for %s' % rule.name)


def execute_rule(rule_, settings):
    def _get_runable_rules(rules, blackboard):
        ready_to_run = []
        for rule in rules:
            ready = True
            for sub_rule in extract_subrules(rule):
                if not blackboard.get(sub_rule.name, None):
                    ready = False
                    break
            if ready:
                ready_to_run.append(rule)

        return ready_to_run

    all_rules = deduplicate_rules(flatten_rules(rule_))
    # initialize the url blackboard, on which each entry is a jobname/outputurl
    # key/value pair. Default value is []
    urls_blackboard = {}
    for rule in all_rules:
        urls_blackboard[rule.name] = []

    # if no sub-rules, execute it as usual
    if len(all_rules) == 1:
        return _run_sequential_rules(all_rules, settings, urls_blackboard)

    # execute all sub-rules concurrently, collect urls for the top-level rule
    pending_rules = all_rules[:-1]
    done = True
    while 1:
        runable_rules = _get_runable_rules(pending_rules, urls_blackboard)
        try:
            ret = _run_concurrent_rules(runable_rules, settings, urls_blackboard)
        except JobError:
            done = False
            break
        for key, value in ret.iteritems():
            urls_blackboard[key] = value
        pending_rules = [rule for rule in pending_rules
                         if rule not in runable_rules]
        if not pending_rules:
            break

    if done:
        return _run_sequential_rules(all_rules[-1:], settings, urls_blackboard)
    else:
        raise JobError('Failed to execute the rule %s' % rule.name)


def execute_immediate_rule(settings):
    immed_rule = settings.get('immediate_rule')
    rules_dir = settings.get('rules_directory')
    rules = get_rules_by_name(immed_rule, rules_dir, immediate=True)
    for rule in rules:
        execute_rule(rule, settings)
