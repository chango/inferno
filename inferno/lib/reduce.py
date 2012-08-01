def keyset_reduce(iter_, params_):

    import ujson

    import disco.util

    class KeysetReduce(object):

        def __init__(self, params):
            self.params = params

        def __call__(self, iter):
            for key, value in disco.util.kvgroup(iter):
                try:
                    key = ujson.loads(key)
                    sum_ = self._sum_group(key, value)

                    # post-process results
                    for xkey, xval in self._post_process(key, sum_):

                        if (hasattr(self.params, 'serial_out') and
                                self.params.serial_out):
                            serial = ','.join([self._safe_str(y) for y in xkey[1:]])
                            result = serial, ujson.dumps(xval)
                        else:
                            result = xkey, xval
                        self._debug('result: %s', result)
                        yield result
                except Exception as e:
                    self._error('error:%s\ninput: %s,%s', e, key, value)

        def _post_process(self, key, val):
            # each post-processor may generate multiple 'parts',
            # these need to be fed into subsequent post-processors
            it = iter(list([(key, val)]))

            keysets = getattr(self.params, 'keysets', None)
            if keysets:
                keyset = keysets.get(key[0], None)
                if keyset and keyset['parts_postprocess']:
                    for func in keyset['parts_postprocess']:
                        it = self._apply_process(it, func)

            for k, v in it:
                yield k, v

        def _apply_process(self, it, func):
            for key, val in it:
                for rkey, rval in func(key, val, self.params):
                    yield rkey, rval

        def _sum_group(self, key, value):
            summed_values = []
            for row in value:
                index = 0
                self._debug('input: %s,%s', key, row)
                for item in row:
                    item = self._convert_to_numeric(item)
                    if index >= len(summed_values):
                        summed_values.append(item)
                    else:
                        summed_values[index] += item
                    index += 1
            return summed_values

        def _convert_to_numeric(self, val):
            if isinstance(val, (float, int, long)):
                return val
            else:
                try:
                    return float(val)
                except:
                    return 0

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

    go = KeysetReduce(params_)
    return go(iter_)
