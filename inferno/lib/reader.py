def keyset_multiplier(stream, size, url, params):
    for in_dict in stream:
        for keyset_name in params.keysets.keys():
            dprime = dict(in_dict)
            dprime['_keyset'] = keyset_name
            yield dprime


def json_reader(stream, size=None, url=None, params=None):
    import ujson
    for line in stream:
        if line.find('{') != -1:
            try:
                parts = ujson.loads(line)
            except:
                # just skip bad lines
                import disco.util
                disco.util.msg('json line error: %r' % line)
                pass
            else:
                yield parts


def csv_reader(stream, size=None, url=None, params=None):
    import csv
    fieldnames = params.csv_fields
    dialect = params.csv_dialect
    for line in stream:
        try:
            reader = csv.DictReader(
                [line], fieldnames=fieldnames, dialect=dialect)
            for parts in reader:
                if None in parts:
                    # remove extra data values
                    del parts[None]
                yield parts
        except:
            # just skip bad lines
            import disco.util
            disco.util.msg('csv line error: %r' % line)
            pass
