from inferno.lib.rule import chunk_csv_stream
from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset


# an example rule parts_preprocess that works for all keysets
def count(parts, params):
    parts['count'] = 1
    yield parts


# an example keyset parts_preprocess that works only for a specific keyset
def count_again(parts, params):
    parts['count'] = parts['count'] + 1
    yield parts

RULES = [
    InfernoRule(
        name='last_names_json',
        source_tags=['example:chunk:users'],
        map_input_stream=chunk_json_stream,
        parts_preprocess=[count],
        partitions=2,
        keysets={
            'last_name_keyset':Keyset(
                key_parts=['last'],
                value_parts=['count'],
                parts_preprocess=[count_again]
             )
        }
    ),
    InfernoRule(
        name='last_names_csv',
        source_tags=['example:chunk:users'],
        map_input_stream=chunk_csv_stream,
        csv_fields=('first', 'last'),
        csv_dialect='excel',
        parts_preprocess=[count],
        partitions=2,
        key_parts=['last'],
        value_parts=['count'],
    ),
    InfernoRule(
        name='last_names_result',
        source_tags=['example:chunk:users'],
        map_input_stream=chunk_json_stream,
        parts_preprocess=[count],
        partitions=2,
        key_parts=['last'],
        value_parts=['count'],
        result_tag='result:test:integration'
    ),
    InfernoRule(
        name='first_and_last_names',
        source_tags=['example:chunk:users'],
        map_input_stream=chunk_json_stream,
        parts_preprocess=[count],
        partitions=2,
        keysets={
            'last_name_keyset':Keyset(
                key_parts=['last'],
                value_parts=['count'],
             ),
            'first_name_keyset':Keyset(
                key_parts=['first'],
                value_parts=['count']
             )
        }
    )
]
