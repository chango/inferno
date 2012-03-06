from inferno.lib.settings import InfernoSettings
from inferno.lib.daemon import InfernoDaemon


class TestInfernoDaemon(object):

    def setUp(self):
        self.settings = InfernoSettings()
        self.daemon = InfernoDaemon(self.settings)
