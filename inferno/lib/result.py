def keyset_result(iter, params, **kwargs):

    import csv
    import sys

    def default_flush_callback(stream):
        stream.flush()
        return stream

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
            columns = params.keysets[keyset_name]['key_parts'][1:]
            columns.extend(params.keysets[keyset_name]['value_parts'])
            writer.writerow(columns)
            last_keyset_name = keyset_name

        if skip_keyset:
            data = keys[1:] + values
        else:
            data = keys + values

        row = [unicode(x).encode('utf-8') for x in data]
        writer.writerow(row)
        count += 1
        if count > flush_schedule:
            output_stream = flush_callback(output_stream)
            count = 0
    flush_callback(output_stream)
