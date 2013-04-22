from nose.tools import eq_
from nose.tools import ok_

from disco.core import result_iterator

from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from inferno.lib.disco_ext import sorted_iterator


class TestInfernoRule(object):

    def test_keysets(self):
#        # no key sets
#        rule = InfernoRule()
#        eq_(rule.params.keysets, {})

        # one key set
        rule = InfernoRule(
            key_parts=['id'],
            value_parts=['count'],
            table='some_table',
            column_mappings={'id': 'some_id'})
        keysets = {
            '_default': {
                'column_mappings': {'id': 'some_id'},
                'table': 'some_table',
                'value_parts': ['count'],
                'key_parts': ['_keyset', 'id'],
                'parts_preprocess': [],
                'parts_postprocess': []}}
        eq_(rule.params.keysets, keysets)

        # many key sets
        rule = InfernoRule(
            keysets={
                'keyset1': Keyset(
                    key_parts=['id1'],
                    value_parts=['count1'],
                    column_mappings={'id1': 'some_id1'},
                    table='some_table1'),
                'keyset2': Keyset(
                    key_parts=['id2'],
                    value_parts=['count2'],
                    column_mappings={'id2': 'some_id2'},
                    table='some_table2')})
        keysets = {
            'keyset1': {
                'column_mappings': {'id1': 'some_id1'},
                'table': 'some_table1',
                'value_parts': ['count1'],
                'key_parts': ['_keyset', 'id1'],
                'parts_preprocess': [],
                'parts_postprocess': [],
            },
            'keyset2': {
                'column_mappings': {'id2': 'some_id2'},
                'table': 'some_table2',
                'value_parts': ['count2'],
                'key_parts': ['_keyset', 'id2'],
                'parts_preprocess': [],
                'parts_postprocess': [],
            },
        }
        eq_(rule.params.keysets, keysets)

    def test_parts_preprocess(self):
        def foo(parts, params):
            parts['bar'] = 1
            yield parts

        rule = InfernoRule(parts_preprocess=[foo])
        eq_(rule.params.parts_preprocess, [foo])
        actual = rule.params.parts_preprocess[0]({'hello': 'world'}, None)
        eq_(list(actual), [{'bar':1, 'hello':'world'}])

    def test_keyset_parts_preprocess(self):
        def foo(parts, params):
            parts['bar'] = 1
            yield parts

        rule = InfernoRule(
                    keysets={
                        'keyset1': Keyset(parts_preprocess=[foo]),
                    })
        funcs = rule.params.keysets['keyset1']['parts_preprocess']
        eq_(funcs, [foo])
        actual = funcs[0]({'hello': 'world'}, None)
        eq_(list(actual), [{'bar':1, 'hello':'world'}])

    def test_field_transforms(self):
        def upper(val):
            return val.upper()

        rule = InfernoRule(field_transforms={'hello': upper})
        eq_(rule.params.field_transforms, {'hello': upper})

    def test_result_iterator(self):
        # sort=default
        rule = InfernoRule()
        eq_(rule.result_iterator, sorted_iterator)

        # sort=True
        rule = InfernoRule(sort=True)
        eq_(rule.result_iterator, sorted_iterator)

        # sort=False
        rule = InfernoRule(sort=False)
        eq_(rule.result_iterator, result_iterator)

    def test_source_tags(self):
        # list
        rule = InfernoRule(source_tags=['tag1', 'tag2'])
        eq_(rule.source_tags, ['tag1', 'tag2'])

        # empty list
        rule = InfernoRule(source_tags=[])
        eq_(rule.source_tags, [])

        # one tag (string)
        rule = InfernoRule(source_tags='tag1')
        eq_(rule.source_tags, ['tag1'])

        # none tag
        rule = InfernoRule(source_tags=None)
        eq_(rule.source_tags, [])

    def test_kwargs(self):
        rule = InfernoRule(some_extra_param='some_extra_value')
        eq_(rule.params.some_extra_param, 'some_extra_value')

    def test_str(self):
        rule = InfernoRule(name='some_rule_name')
        eq_(str(rule), '<InfernoRule: some_rule_name>')

#    def test_source_urls(self):
#        from functools import partial
#
#        def partial_rules(rule, arg_str, arg_int):
#            return [str(rule.name == 'sourcy'), str(arg_str == 'arg1'), str(arg_int == 15)]
#
#        def func_rules(rule):
#            return ['aurl']
#
#        rule = InfernoRule(name='sourcy', source_urls=partial(partial_rules, arg_str='arg1', arg_int=15))
#        for val in rule.source_urls:
#            ok_(val)
#
#        rule = InfernoRule(name='sourcy1', source_urls=func_rules)
#        eq_(rule.source_urls, ['aurl'])
#
#        rule = InfernoRule(name='sourcy2', source_urls=('url1', 'url2'))
#        eq_(rule.source_urls, ('url1', 'url2'))
