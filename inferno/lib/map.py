def keyset_map(parts_, params_):

    import ujson

    class KeysetMap(object):

        def __init__(self, params):
            self.params = params

        def __call__(self, parts):
            try:
                self._debug('input: %s', parts)
                for parts in self._preprocess([parts]):
                    self._transform(parts)
                    result = self._result(parts)
                    if result is not None:
                        self._debug('result: %s', result)
                        yield result
            except Exception:
                self._error('input: %s', parts_)

        def _preprocess(self, parts_list):
            if hasattr(self.params, 'parts_preprocess'):
                # each preprocessor may generate multiple 'parts',
                # these need to be fed into subsequent preprocessors
                for name in self.params.parts_preprocess:
                    func = getattr(self.params, name)
                    new_list = []
                    for parts in parts_list:
                        new_list.extend([x for x in func(parts, self.params)])
                    parts_list = new_list
            return parts_list

        def _transform(self, parts):
            if hasattr(self.params, 'field_transforms'):
                for field, name in self.params.field_transforms.items():
                    func = getattr(self.params, name)
                    if field in parts:
                        parts[field] = func(parts[field])

        def _result(self, parts):
            keyset = parts.get('_keyset', '_default')
            if keyset in self.params.keysets:
                key_parts = self.params.keysets[keyset]['key_parts']
                value_parts = self.params.keysets[keyset]['value_parts']
                key = tuple(self._make_key(parts, key_parts))
                if None not in key[1:]:
                    value = [parts.get(a, 0) for a in value_parts]
                    try:
                        return ujson.dumps(key), value
                    except Exception:
                        self._error(': %s', parts_)

        def _make_key(self, parts, keys):
            result = []
            for key in keys:
                if key in parts and parts[key] is not None:
                    result.append(self._safe_str(parts[key]))
                else:
                    result.append(None)
            return result

        def _safe_str(self, value):
            try:
                return str(value)
            except UnicodeEncodeError:
                return unicode(value).encode('utf-8')

        def _debug(self, message, *args):
            if getattr(self.params, 'disco_debug', False):
                self._disco_message(message % args)

        def _error(self, message, *args):
            import traceback
            trace = traceback.format_exc(15)
            self._disco_message('%s %s' % (message, trace))

        def _disco_message(self, message):
            import disco.util
            disco.util.msg(message)

    go = KeysetMap(params_)
    return go(parts_)
