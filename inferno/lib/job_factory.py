from inferno.lib.disco_ext import get_disco_handle
from inferno.lib.job import InfernoJob
from inferno.lib.lookup_rules import get_rules_by_name


class JobFactory(object):

    @staticmethod
    def get_immediate_rules(settings):
        immed_rule = settings.get('immediate_rule')
        rules_dir = settings.get('rules_directory')
        return get_rules_by_name(immed_rule, rules_dir, immediate=True)

    @staticmethod
    def execute_immediate_rule(settings, parent=None):
        rules = JobFactory.get_immediate_rules(settings)
        for rule in rules:
            job = JobFactory.create_job(rule, settings, parent)
            if job.start():
                job.wait()

    @staticmethod
    def execute_immediate(parent=None, settings=None, **kwargs):
        if not settings:
            settings = kwargs
        else:
            settings.update(kwargs)
        JobFactory.execute_immediate_rule(settings, parent)

    @staticmethod
    def get_disco_ddfs(settings):
        server = settings.get('server')
        return get_disco_handle(server)

    @staticmethod
    def create_job(rule, settings, parent=None):
        return InfernoJob(rule, settings, parent)
