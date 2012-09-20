def keyset_multiplier(stream, size, url, params):
    for in_dict in stream:
        for keyset_name in params.keysets.keys():
            try:
                dprime = dict(in_dict)
            except Exception as e:
                print "KEYSET Parts Error: %s" % in_dict
            else:
                dprime['_keyset'] = keyset_name
                yield dprime


def json_reader(stream, size=None, url=None, params=None):
    import ujson
    for line in stream:
        if line.find('{') != -1:
            try:
                parts = ujson.loads(line)
                assert isinstance(parts, dict)
            except:
                # just skip bad lines
                import disco.util
                disco.util.msg('json line error: %r' % line)
            else:
                yield parts


def csv_reader(stream, size=None, url=None, params=None):
    import csv
    import __builtin__

    fieldnames = getattr(params, 'csv_fields', None)
    dialect = getattr(params, 'csv_dialect', 'excel')

    reader = csv.reader(stream, dialect=dialect)
    done = False
    while not done:
        try:
            line = reader.next()
            if not line:
                continue
            if not fieldnames:
                fieldnames = [str(x) for x in range(len(line))]
            parts = dict(__builtin__.map(None, fieldnames, line))
            if None in parts:
                # remove extra data values
                del parts[None]
            yield parts
        except StopIteration as e:
            done = True
        except Exception as ee:
            # just skip bad lines
            import disco.util
            disco.util.msg('csv line error: %s' % ee)
