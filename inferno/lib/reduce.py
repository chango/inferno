def keyset_reduce(iter_, params_):

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

    def _apply_process(params, it, func):
        for key, val in it:
            for rkey, rval in func(key, val, params):
                yield rkey, rval

    def _post_process(params, key, val):
        # each post-processor may generate multiple 'parts',
        # these need to be fed into subsequent post-processors
        it = iter(list([(key, val)]))

        keysets = getattr(params, 'keysets', None)
        if keysets:
            keyset = keysets.get(key[0], None)
            if keyset and keyset['parts_postprocess']:
                for func in keyset['parts_postprocess']:
                    it = _apply_process(params, it, func)

        for k, v in it:
            yield k, v

    def _convert_to_numeric(val):
        if isinstance(val, (float, int, long)):
            return val
        else:
            try:
                return float(val)
            except:
                return 0

    def _sum_group(key, value):
        summed_values = []
        for row in value:
            index = 0
            _inferno_debug('input: %s,%s', key, row)
            for item in row:
                item = _convert_to_numeric(item)
                if index >= len(summed_values):
                    summed_values.append(item)
                else:
                    summed_values[index] += item
                index += 1
        return summed_values

    # keyset_reduce function begins
    init_func = getattr(params_, 'reduce_init_function', None)
    if init_func:
        init_func(iter_, params_)

    for key, value in disco.util.kvgroup(iter_):
        try:
            key = ujson.loads(key)
            sum_ = _sum_group(key, value)

            # post-process results
            for xkey, xval in _post_process(params_, key, sum_):
                if (hasattr(params_, 'serial_out') and
                        params_.serial_out):
                    serial = ','.join([_safe_str(y) for y in xkey[1:]])
                    result = serial, ujson.dumps(xval)
                else:
                    result = xkey, xval
                _inferno_debug('result: %s', result)
                yield result
        except Exception as e:
            _inferno_error('error:%s\ninput: %s,%s', e, key, value)
