from disco import func
from disco import util
from disco.settings import DiscoSettings

from inferno.lib.sorted_iterator import SortedIterator


def get_disco_handle(server):
    from disco.core import Disco
    from disco.ddfs import DDFS

    if server and not server.startswith('disco://'):
        server = 'disco://' + server

    if server and ':' not in server:
        server = server + ":8989"

    return Disco(server), DDFS(server)


def sorted_iterator(urls,
                    reader=func.chain_reader,
                    input_stream=(func.map_input_stream,),
                    notifier=func.notifier,
                    params=None,
                    ddfs=None):

    from disco.worker import Input
    from disco.worker.classic.worker import Worker

    worker = Worker(map_reader=reader, map_input_stream=input_stream)
    settings = DiscoSettings(DISCO_MASTER=ddfs) if ddfs else DiscoSettings()

    inputs = []
    for input in util.inputlist(urls, settings=settings):
        notifier(input)
        instream = Input(input, open=worker.opener('map', 'in', params))
        if instream:
            inputs.append(instream)

    return SortedIterator(inputs)


def json_output_stream(stream, partition, url, params):
    from disco.fileutils import DiscoOutputStream_v1
    import ujson

    #print "PZRIXMS: %s" % params.__dict__

    class JsonOutputStream(DiscoOutputStream_v1):
        def __init__(self, stream, params, **kwargs):
            super(JsonOutputStream, self).__init__(stream, **kwargs)
            self.params = params

        def add(self, k, v):
            # just convert key and value tuples to a dict, then append
            # note we need to use the _keyset to determine how to build the dicts
            keyset = self.params.keysets[k[0]]
            record = dict(zip(keyset['key_parts'], k) + zip(keyset['value_parts'], v))
            self.append(ujson.dumps(record))

    return JsonOutputStream(stream, params)


