def keyset_result(iter, params, **kwargs):

    import csv
    import sys

    def _column_map(keyset, name):
        if keyset.get('column_mappings', None):
            mappings = keyset['column_mappings']
            if name in mappings and mappings[name]:
                return mappings[name]
        return name

    def _post_process(parts_list, params):
        if hasattr(params, 'parts_postprocess'):
            # each post-processor may generate multiple 'parts',
            # these need to be fed into subsequent post-processors
            for name in params.parts_postprocess:
                func = getattr(params, name)
                new_list = []
                for parts in parts_list:
                    new_list.extend([x for x in func(parts, params)])
                parts_list = new_list
        return parts_list

    mapped = []
    last_keyset_name = None
    writer = csv.writer(sys.stdout)
    for keys, values in iter:
        keyset_name = keys[0]
        if last_keyset_name != keyset_name:
            keyset = params.keysets[keyset_name]
            columns = keyset['key_parts'][1:] + keyset['value_parts']
            mapped = [_column_map(keyset, column) for column in columns]
            writer.writerow(mapped)
            last_keyset_name = keyset_name
        parts = dict(zip(mapped, keys[1:] + values))
        for parts in _post_process([parts], params):
            data = [parts.get(column) for column in mapped]
            row = [unicode(x).encode('utf-8') for x in data]
            writer.writerow(row)


def reduce_result(iter, **kwargs):

    import csv
    import sys

    def default_flush_callback(stream):
        stream.flush()
        return stream

    output_stream = kwargs.get('output_stream', sys.stdout)
    flush_schedule = kwargs.get('flush_schedule', 5000)
    flush_callback = kwargs.get('flush_callback', default_flush_callback)

    writer = csv.writer(output_stream)
    count = 0
    for keys, values in iter:
        data = keys[1:] + values
        row = [unicode(x).encode('utf-8') for x in data]
        writer.writerow(row)
        count += 1
        if count > flush_schedule:
            output_stream = flush_callback(output_stream)
            count = 0
    flush_callback(output_stream)
