from inferno.lib.rule import chunk_json_stream, json_reduce_output_stream
from inferno.lib.rule import InfernoRule

AUTORUN = True

def count(parts, params):
    parts['count'] = 1
    yield parts

def processor(iter, **params):
    with open("/tmp/tst_copy_of_results", 'a') as f:
        for k, v in iter:
            print >>f, k, v

RULES = [
    InfernoRule(
        name='count_names',
        source_tags=[
            InfernoRule(
                name='__count_names',
                map_input_stream=chunk_json_stream,
                source_tags=['example:chunk:users'],
                parts_preprocess=[count],
                key_parts=['first', 'last'],
                value_parts=['count'],
                result_tag="__count_names_tag",
                result_processor=None,
                reduce_output_stream=json_reduce_output_stream,
                )
            ],
        map_input_stream=chunk_json_stream,
        partitions=2,
        key_parts=['first', 'last'],
        value_parts=['count'],
        result_processor=processor,
        result_tag='result:test:integration'
    )
]
