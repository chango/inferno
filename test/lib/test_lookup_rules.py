import os

from nose.tools import eq_

from inferno.lib.lookup_rules import get_rules
from inferno.lib.lookup_rules import get_rule_modules
from inferno.lib.lookup_rules import get_rules_by_name


class TestGetRulesByName(object):

    def setUp(self):
        here = os.path.dirname(__file__)
        self.rules_dir = os.path.join(here, '..', 'fixture', 'fake_rules')

    def test_empty_name(self):
        self._assert_get_rules_by_name('', [])

    def test_unknown_module(self):
        name = 'some_unknown_module'
        self._assert_get_rules_by_name(name, [])

    def test_unknown_rule(self):
        name = 'automatic.some_unknown_rule'
        self._assert_get_rules_by_name(name, [])

    def test_unknown_keyset(self):
        name = 'automatic.automatic_rule_1.some_unknown_keyset'
        self._assert_get_rules_by_name(name, [])

    def test_automatic_module(self):
        name = 'automatic'
        expected = [
            ['automatic_rule_1', ['keyset_1', 'keyset_2']],
            ['automatic_rule_2', ['keyset_1', 'keyset_2']],
            ['automatic_rule_3', ['_default']]]
        self._assert_get_rules_by_name(name, expected)
        self._assert_get_rules_by_name(name, expected, immediate=False)
        self._assert_get_rules_by_name(name, expected, immediate=True)

    def test_automatic_module_rule(self):
        name = 'automatic.automatic_rule_1'
        expected = [['automatic_rule_1', ['keyset_1', 'keyset_2']]]
        self._assert_get_rules_by_name(name, expected)
        self._assert_get_rules_by_name(name, expected, immediate=False)
        self._assert_get_rules_by_name(name, expected, immediate=True)

    def test_automatic_module_rule_keyset(self):
        name = 'automatic.automatic_rule_1.keyset_2'
        expected = [['automatic_rule_1', ['keyset_2']]]
        self._assert_get_rules_by_name(name, expected)
        self._assert_get_rules_by_name(name, expected, immediate=False)
        self._assert_get_rules_by_name(name, expected, immediate=True)

    def test_manual_module(self):
        name = 'manual'
        expected = [
            ['manual_rule_1', ['keyset_1', 'keyset_2']],
            ['manual_rule_2', ['keyset_1', 'keyset_2']],
            ['manual_rule_3', ['_default']]]
        self._assert_get_rules_by_name(name, [])
        self._assert_get_rules_by_name(name, [], immediate=False)
        self._assert_get_rules_by_name(name, expected, immediate=True)

    def test_manual_module_rule(self):
        name = 'manual.manual_rule_1'
        expected = [['manual_rule_1', ['keyset_1', 'keyset_2']]]
        self._assert_get_rules_by_name(name, [])
        self._assert_get_rules_by_name(name, [], immediate=False)
        self._assert_get_rules_by_name(name, expected, immediate=True)

    def test_manual_module_rule_keyset(self):
        name = 'manual.manual_rule_1.keyset_2'
        expected = [['manual_rule_1', ['keyset_2']]]
        self._assert_get_rules_by_name(name, [])
        self._assert_get_rules_by_name(name, [], immediate=False)
        self._assert_get_rules_by_name(name, expected, immediate=True)

    def _assert_get_rules_by_name(self, name, expected, immediate=None):
        if immediate is None:
            rules = get_rules_by_name(name, self.rules_dir)
        else:
            rules = get_rules_by_name(name, self.rules_dir, immediate)
        actual = [[rule.name, rule.params.keysets.keys()] for rule in rules]
        for rule in actual:
            rule[1].sort()
        eq_(actual, expected)


class TestLookupRules(object):

    def setUp(self):
        here = os.path.dirname(__file__)
        self.rules_dir = os.path.join(here, '..', 'fixture', 'fake_rules')

    def test_get_rule_modules(self):
        actual = get_rule_modules(self.rules_dir)
        expected = [
            'automatic',
            'manual',
            'more_automatic',
            'more_manual']
        eq_(actual, expected)

    def test_get_automatic_rules(self):
        rules = get_rules(self.rules_dir)
        actual = [rule.name for rule in rules]
        expected = [
            'automatic_rule_1',
            'automatic_rule_2',
            'automatic_rule_3',
            'automatic_rule_4']
        eq_(actual, expected)
