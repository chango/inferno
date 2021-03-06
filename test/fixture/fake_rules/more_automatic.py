from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset


AUTORUN = True

RULES = [
    InfernoRule(
        name='automatic_rule_4',
        keysets={
            'keyset_1':Keyset(
                key_parts=['key_1'],
                value_parts=['value_1'],
             ),
            'keyset_2':Keyset(
                key_parts=['key_2'],
                value_parts=['value_2']
             )
        }
    ),
]
