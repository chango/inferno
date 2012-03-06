from datetime import date

from nose.tools import eq_

from inferno.lib.job_options import JobOptions
from inferno.lib.rule import InfernoRule
from inferno.lib.settings import InfernoSettings


class TestJobOptions(object):

    def setUp(self):
        self.rule = InfernoRule(
            day_range=3,
            day_offset=1,
            day_start=date(2012, 12, 02),
            source_tags=['tag1', 'tag2'],
            result_tag='result_tag_rule')
        self.settings = InfernoSettings(
            day_range=4,
            day_offset=2,
            day_start=date(2011, 12, 02),
            source_tags=['tag3', 'tag4'],
            result_tag='result_tag_settings')

        # expected results
        self.result_tag_from_rule = 'result_tag_rule'
        self.result_tag_from_settings = 'result_tag_settings'
        self.tags_from_rule = [
                'tag1:2012-12-01',
                'tag1:2012-11-30',
                'tag1:2012-11-29',
                'tag2:2012-12-01',
                'tag2:2012-11-30',
                'tag2:2012-11-29']
        self.tags_from_settings = [
                'tag3:2011-11-30',
                'tag3:2011-11-29',
                'tag3:2011-11-28',
                'tag3:2011-11-27',
                'tag4:2011-11-30',
                'tag4:2011-11-29',
                'tag4:2011-11-28',
                'tag4:2011-11-27']

    def test_empty_rule_and_empty_settings(self):
        job_options = JobOptions(InfernoRule(), InfernoSettings())
        eq_(job_options.tags, [])
        eq_(job_options.result_tag, None)

    def test_result_tag_from_rule(self):
        actual = JobOptions(self.rule, InfernoSettings()).result_tag
        eq_(actual, self.result_tag_from_rule)

    def test_result_tag_from_settings(self):
        actual = JobOptions(InfernoRule(), self.settings).result_tag
        eq_(actual, self.result_tag_from_settings)

    def test_result_tag_settings_trump_rule(self):
        actual = JobOptions(self.rule, self.settings).result_tag
        eq_(actual, self.result_tag_from_settings)

    def test_tags_from_rule(self):
        actual = JobOptions(self.rule, InfernoSettings()).tags
        eq_(actual, self.tags_from_rule)

    def test_tags_from_settings(self):
        actual = JobOptions(InfernoRule(), self.settings).tags
        eq_(actual, self.tags_from_settings)

    def test_tags_settings_trump_rule(self):
        actual = JobOptions(self.rule, self.settings).tags
        eq_(actual, self.tags_from_settings)

    def test_tags_from_settings_and_rule_mix(self):
        rule = InfernoRule(source_tags=['tag5'], day_range=2)
        settings = InfernoSettings(day_start=date(2011, 12, 01))
        actual = JobOptions(rule, settings).tags
        expected = ['tag5:2011-12-01', 'tag5:2011-11-30']
        eq_(actual, expected)

    def test_explict_tags(self):
        rule = InfernoRule(source_tags=['tag:foo', 'tag:bar'])
        settings = InfernoSettings()
        actual = JobOptions(rule, settings).tags
        expected = ['tag:foo', 'tag:bar']
        eq_(actual, expected)

    def test_explict_tags_despite_day_range_on_the_rule(self):
        rule = InfernoRule(source_tags=['tag:foo', 'tag:bar'], day_range=2)
        settings = InfernoSettings(day_range=0)
        actual = JobOptions(rule, settings).tags
        expected = ['tag:foo', 'tag:bar']
        eq_(actual, expected)
