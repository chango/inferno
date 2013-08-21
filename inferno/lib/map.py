def keyset_map(parts_, params_):

    import ujson
    import disco.util

    def _safe_str(value):
        try:
            return str(value)
        except UnicodeEncodeError:
            return unicode(value).encode('utf-8')

    def _disco_message(message):
        disco.util.msg(message)

    def _inferno_debug(params, message, *args):
        if getattr(params, 'disco_debug', False):
            _disco_message(message % args)

    def _inferno_error(message, *args):
        import traceback
        trace = traceback.format_exc(15)
        _disco_message('%s %s' % (message, trace))

    # parts_preprocess for the whole rule
    def _preprocess(params, parts_list):
        if hasattr(params, 'parts_preprocess'):
            # each preprocessor may generate multiple 'parts',
            # these need to be fed into subsequent preprocessors
            for func in params.parts_preprocess:
                new_list = []
                for parts in parts_list:
                    new_list.extend([x for x in func(parts, params)])
                parts_list = new_list
        return parts_list

    def _transform(params, parts):
        if hasattr(params, 'field_transforms'):
            for field, func in params.field_transforms.items():
                if field in parts:
                    parts[field] = func(parts[field])

    def _make_key(parts, keys):
        result = []
        for key in keys:
            if key in parts and parts[key] is not None:
                result.append(_safe_str(parts[key]))
            else:
                result.append(None)
        return result

    def _keyset_preprocess(keyset, params, parts_list):
        ''' parts_preprocess for a specific keyset '''
        if keyset.get('parts_preprocess', False):
            for func in keyset['parts_preprocess']:
                new_list = [parts_list.get('_keyset')]
                for parts in parts_list:
                    new_list.extend([x for x in func(parts, params)])
                parts_list = new_list
        return parts_list

    def _keyset_multiplier(params_, parts_):
        for keyset_name in params_.keysets.keys():
            try:
                dprime = dict(parts_)
            except Exception:
                print "KEYSET Parts Error: %s" % parts_
            else:
                dprime['_keyset'] = keyset_name
                yield dprime

    def _result(params, parts):
        keyset = parts.get('_keyset', '_default')
        if keyset in params.keysets:
            # Keyset specific preprocessor
            # Note that following is an one-element loop to make Disco happy, otherwise,
            # it ends up with a Disco runtime error.
            for parts in _keyset_preprocess(params.keysets[keyset], params, [parts]):
                key_parts = params.keysets[keyset]['key_parts']
                value_parts = params.keysets[keyset]['value_parts']
                key = tuple(_make_key(parts, key_parts))
                if None not in key[1:]:
                    value = [parts.get(a, 0) for a in value_parts]
                    try:
                        yield ujson.dumps(key), value
                    except Exception:
                        _inferno_error(': %s', parts)

    # keyset_map function begins
    try:
        _inferno_debug(params_, 'input: %s', parts_)
        for parts in _preprocess(params_, [parts_]):
            _inferno_debug(params_, 'postprocess: %s', parts)
            _transform(params_, parts)
            _inferno_debug(params_, 'posttransform: %s', parts)
            # use keyset multiplier to generate keyset specific parts
            for keyset_parts in _keyset_multiplier(params_, parts):
                results = _result(params_, keyset_parts)
                if results is not None:
                    for result in results:
                        _inferno_debug(params_, 'result: %s', result)
                        yield result
    except Exception:
        _inferno_error('input: %s', parts_)
