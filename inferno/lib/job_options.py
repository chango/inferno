from datetime import timedelta
import functools
import types

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

        if self.settings.get('source_tags') is not None:
            tags = self.settings.get('source_tags')
        else:
            tags = self.rule.source_tags

            # note that all day range options are disabled if we pass tags in
            # on the command line
            if self.settings.get('day_start') is not None:
                start = self.settings.get('day_start')
            else:
                start = self.rule.day_start

            if self.settings.get('day_offset') is not None:
                start += timedelta(days=-self.settings.get('day_offset'))
            else:
                start += timedelta(days=-self.rule.day_offset)

            if self.settings.get('day_range') is not None:
                count = self.settings.get('day_range')
            else:
                count = self.rule.day_range

            if count and tags:
                tags = [self._name(tag, day, start)
                    for tag in tags for day in range(count)]

        return tags or []

    @property
    def urls(self):
        urls_or_func = self.settings.get('source_urls')
        rval = urls_or_func
        if urls_or_func and (isinstance(urls_or_func, types.FunctionType) or
                             isinstance(urls_or_func, functools.partial)):
            rval = urls_or_func(self)
        return rval or []


    def _name(self, tag, delta, start):
        return '%s:%s' % (tag, start + timedelta(days=-delta))
