import copy
import imp
import logging
import os


log = logging.getLogger(__name__)


def get_rules_by_name(name, rules_directory, immediate=False):

    def _get_rule_module(rules_dir, path, mod_name):
        try:
            file, path, desc = imp.find_module(mod_name, [rules_dir])
            mod = imp.load_module(mod_name, file, path, desc)
            return mod
        except Exception as e:
            import traceback
            trace = traceback.format_exc(15)
            log.warning("%s %s" % (e, trace))
            return None

    def _rule_with_requested_keyset(keyset_name, rule):
        rule_copy = copy.deepcopy(rule)
        for keyset_key in rule_copy.params.keysets.keys():
            if keyset_key != keyset_name:
                del rule_copy.params.keysets[keyset_key]
        return rule_copy

    path = name.split('.')
    mod_name = path[0] if len(path) > 0 else None
    rule_name = path[1] if len(path) > 1 else None
    keyset_name = path[2] if len(path) > 2 else None

    mod = _get_rule_module(rules_directory, path, mod_name)
    if not immediate and mod and not getattr(mod, 'AUTORUN', False):
        log.debug('Not immediate mode, skipping rules for %r', name)
        return []

    rules = getattr(mod, 'RULES', [])
    if rule_name and keyset_name:
        for rule in rules:
            if rule.name == rule_name and keyset_name in rule.params.keysets:
                rule_copy = _rule_with_requested_keyset(keyset_name, rule)
                rule_copy.qualified_name = '%s.%s' % (mod, rule.name)
                return [rule_copy]
    elif rule_name:
        for rule in rules:
            if rule.name == rule_name:
                rule.qualified_name = '%s.%s' % (mod_name, rule.name)
                return [rule]
    elif mod and rules:
        return rules

    log.warning('Unable to find rules for %r', name)
    return []


def get_rule_modules(rules_directory):
    modules = []
    for name in os.listdir(rules_directory):
        if name.startswith('.'):
            continue
        name, ext = os.path.splitext(name)
        if ext == '.py' and name != '__init__':
            modules.append(name)
    return modules


def get_rules(rules_directory, immediate=False):
    rules = []
    for mod in get_rule_modules(rules_directory):
        for rule in get_rules_by_name(mod, rules_directory, immediate):
            rule.qualified_name = '%s.%s' % (mod, rule.name)
            rules.append(rule)
    return rules


def get_rule_dict(rules_directory, immediate=False):
    rules = {}
    for mod in get_rule_modules(rules_directory):
        for rule in get_rules_by_name(mod, rules_directory, immediate):
            rule.qualified_name = '%s.%s' % (mod, rule.name)
            mod_list = rules.setdefault(mod, [])
            mod_list.append(rule)
    return rules
