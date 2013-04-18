from datetime import timedelta, date
import functools
import types
from inferno.lib.rule import InfernoRule


class JobOptions(object):

    def __init__(self, rule, settings):
        self.rule = rule
        self.settings = settings

    @property
    def result_tag(self):
        if self.settings.get('result_tag'):
            return self.settings.get('result_tag')
        else:
            return self.rule.result_tag

    @property
    def tags(self):
        def _filter_rules(tags):
            return [tag for tag in tags if not isinstance(tag, InfernoRule)]

        if self.settings.get('source_tags') is not None:
            tags = _filter_rules(self.settings.get('source_tags'))
        else:
            tags = _filter_rules(self.rule.source_tags)

            # note that all day range options are disabled if we pass tags in
            # on the command line
            count = None
            if self.settings.get('day_start') is not None:
                start = self.settings.get('day_start')
                count = 1
            elif self.rule.day_start is not None:
                start = self.rule.day_start
            else:
                start = date.today()

            if self.settings.get('day_offset') is not None:
                start += timedelta(days=-self.settings.get('day_offset'))
                count = 1
            else:
                start += timedelta(days=-self.rule.day_offset)

            if self.settings.get('day_range') is not None:
                count = self.settings.get('day_range')
            elif count is None:
                count = self.rule.day_range

            if count and tags:
                tags = [self._name(tag, day, start)
                    for tag in tags for day in range(count)]

        return tags or []

    @property
    def urls(self):
        import operator

        urls_or_func = self.settings.get('source_urls') or getattr(self.rule, 'source_urls', None)
        rval = urls_or_func
        if operator.isCallable(urls_or_func):
            rval = urls_or_func(self)
        return rval or []


    def _name(self, tag, delta, start):
        return '%s:%s' % (tag, start + timedelta(days=-delta))
