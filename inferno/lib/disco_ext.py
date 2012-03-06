from disco import func
from disco import util
from disco.settings import DiscoSettings

from inferno.lib.sorted_iterator import SortedIterator


def get_disco_handle(server):
    from disco.core import Disco
    from disco.ddfs import DDFS

    if server and not server.startswith('disco://'):
        server = 'disco://' + server

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

    sorted_iter = SortedIterator(inputs)
    for item in sorted_iter:
        yield item
