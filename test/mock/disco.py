import os
import json
import random


def fixture_path(name):
    here = os.path.dirname(__file__)
    return os.path.join(here, '..', 'fixture', name)


class Job(object):

    def __init__(self, name, *args, **kwargs):
        self.name = '%s@%.5f' % (name, random.random())
        self.options = kwargs

    def profile_stats(self):
        raise NotImplemented

    def wait(self):
        pass


class Disco(object):

    def __init__(self, server=None):
        self.server = server
        self.master = server

    def new_job(self, *args, **kwargs):
        return Job(*args, **kwargs)

    def purge(self, job_name):
        pass


class DDFS(object):

    def __init__(self, server=None):
        self.server = server
        with open(fixture_path('ddfs.json')) as f:
            self.ddfs = json.loads(f.read())
        for tag, blobs in self.ddfs.iteritems():
            self.ddfs[tag] = [tuple(x) for x in blobs]

    def list(self, tag):
        return [key for key in self.ddfs.iterkeys() if key.startswith(tag)]

    def blobs(self, tag):
        return self.ddfs.get(tag, [])

    def tag(self, tag, target):
        self.ddfs.setdefault(tag, []).extend(target)
