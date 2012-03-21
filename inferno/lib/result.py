def keyset_result(iter, params, **kwargs):

    import csv
    import sys

    def default_flush_callback(stream):
        stream.flush()
        return stream

    def _column_map(keyset, name):
        if keyset.get('column_mappings', None):
            mappings = keyset['column_mappings']
            if name in mappings and mappings[name]:
                return mappings[name]
        return name

    def _post_process(result_list, params):
        if hasattr(params, 'result_postprocess'):
            # each post-processor may generate multiple 'results',
            # these need to be fed into subsequent post-processors
            for name in params.result_postprocess:
                func = getattr(params, name)
                new_list = []
                for results in result_list:
                    new_list.extend([x for x in func(results, params)])
                result_list = new_list
        return result_list

    output_stream = kwargs.get('output_stream', sys.stdout)
    flush_schedule = kwargs.get('flush_schedule', 5000)
    flush_callback = kwargs.get('flush_callback', default_flush_callback)
    generate_header = kwargs.get('generate_header', True)
    skip_keyset = kwargs.get('skip_keyset', True)

    writer = csv.writer(output_stream)
    last_keyset_name = None
    count = 0
    for keys, values in iter:
        keyset_name = keys[0]
        if generate_header and last_keyset_name != keyset_name:
            keyset = params.keysets[keyset_name]
            columns = keyset['key_parts'][1:] + keyset['value_parts']
            mapped = [_column_map(keyset, column) for column in columns]
            writer.writerow(mapped)
            last_keyset_name = keyset_name

        data = keys + values
        for data in _post_process([data], params):
            if skip_keyset:
                data = data[1:]
            row = [unicode(x).encode('utf-8') for x in data]
            writer.writerow(row)
            count += 1
            if count > flush_schedule:
                output_stream = flush_callback(output_stream)
                count = 0
    flush_callback(output_stream)
