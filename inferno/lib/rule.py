import sys

from datetime import date

from disco.core import Params
from disco.core import result_iterator
from disco.func import chain_stream
from disco.func import disco_output_stream
from disco.func import gzip_stream
from disco.func import map_output_stream
from disco.func import reduce_output_stream

from inferno.lib.disco_ext import sorted_iterator
from inferno.lib.map import keyset_map
from inferno.lib.reader import csv_reader
from inferno.lib.reader import json_reader
from inferno.lib.reader import keyset_multiplier
from inferno.lib.reduce import keyset_reduce
from inferno.lib.result import keyset_result


gzip_csv_stream = gzip_stream + (csv_reader,)
gzip_json_stream = gzip_stream + (json_reader,)
chunk_json_stream = chain_stream + (json_reader,)
chunk_json_keyset_stream = chain_stream + (json_reader, keyset_multiplier)
chunk_csv_stream = chain_stream + (csv_reader,)
chunk_csv_keyset_stream = chain_stream + (csv_reader, keyset_multiplier)


class Keyset(object):

    def __init__(self,
                 key_parts=None,
                 value_parts=None,
                 column_mappings=None,
                 table=None):

        self.key_parts = ['_keyset'] + list(key_parts)
        self.value_parts = value_parts
        self.column_mappings = column_mappings
        self.table = table

    def as_dict(self):
        return {
            'key_parts': self.key_parts,
            'value_parts': self.value_parts,
            'column_mappings': self.column_mappings,
            'table': self.table}


class InfernoRule(object):

    def __init__(self,
                 # name, on/off
                 name='_unnamed_',
                 run=True,

                 # throttle
                 min_blobs=1,
                 max_blobs=sys.maxint,
                 partitions=200,
                 scheduler=None,
                 time_delta=None,

                 # archive
                 archive=False,
                 archive_tag_prefix='processed',

                 # map
                 map_init_function=lambda x, y: x,
                 map_function=keyset_map,
                 map_input_stream=chunk_csv_keyset_stream,
                 map_output_stream=(
                     map_output_stream, disco_output_stream),

                 # reduce
                 reduce_function=keyset_reduce,
                 reduce_output_stream=(
                     reduce_output_stream, disco_output_stream),

                 # result
                 result_processor=keyset_result,
                 result_tag=None,
                 save=False,
                 sort=True,

                 # keysets
                 keysets=None,
                 key_parts=None,
                 value_parts=None,
                 column_mappings=None,
                 table=None,

                 # input
                 day_range=0,
                 day_offset=0,
                 day_start=date.today(),
                 source_tags=None,

                 # other
                 rule_init_function=None,
                 parts_preprocess=None,
                 parts_postprocess=None,
                 field_transforms=None,
                 **kwargs):

        self.qualified_name = name
        if kwargs:
            self.params = Params(**kwargs)
        else:
            self.params = Params()

        if not scheduler:
            scheduler = {'force_local': True, 'max_cores': 200}

        # name, on/off
        self.run = run
        self.name = name

        # throttle
        self.min_blobs = min_blobs
        self.max_blobs = max_blobs
        self.partitions = partitions
        self.scheduler = scheduler
        self.time_delta = time_delta
        if self.time_delta is None:
            self.time_delta = {'minutes': 5}

        # archive
        self.archive = archive
        self.archive_tag_prefix = archive_tag_prefix

        # map
        self.map_init_function = map_init_function
        self.map_function = map_function
        self.map_input_stream = map_input_stream
        self.map_output_stream = map_output_stream

        # reduce
        self.reduce_function = reduce_function
        self.reduce_output_stream = reduce_output_stream

        # result
        self.result_processor = result_processor
        self.result_tag = result_tag
        self.save = save
        self.sort = sort
        if self.sort:
            self.result_iterator = sorted_iterator
        else:
            self.result_iterator = result_iterator

        # input
        if isinstance(source_tags, basestring):
            source_tags = [source_tags]
        self.day_range = day_range
        self.day_offset = day_offset
        self.day_start = day_start
        self.source_tags = source_tags

        # keysets
        keyset_dict = {}
        if keysets:
            for keyset_name, keyset_obj in keysets.items():
                keyset_dict[keyset_name] = keyset_obj.as_dict()
        elif key_parts and value_parts:
            keyset_dict['_default'] = Keyset(
                key_parts,
                value_parts,
                column_mappings,
                table).as_dict()
        self.params.keysets = keyset_dict

        # preprocess
        if parts_preprocess:
            self.params.parts_preprocess = map(
                lambda func: func.__name__, parts_preprocess)
            for func in parts_preprocess:
                self.params.__setattr__(func.__name__, func)
        else:
            self.params.parts_preprocess = []

        # postprocess
        if parts_postprocess:
            self.params.parts_postprocess = map(
                lambda func: func.__name__, parts_postprocess)
            for func in parts_postprocess:
                self.params.__setattr__(func.__name__, func)
        else:
            self.params.parts_postprocess = []

        # transforms
        if field_transforms:
            self.params.field_transforms = {}
            for key, func in field_transforms.items():
                self.params.field_transforms[key] = func.__name__
                self.params.__setattr__(func.__name__, func)

        # other
        self.rule_init_function = rule_init_function

    def __str__(self):
        return '<InfernoRule: %s>' % self.name

    @property
    def is_atomic(self):
        return self.archive

    def summary_dict(self):
        def fstr(func):
            return "%s.%s" % (func.__module__, func.__name__)

        def fname(funcvec):
            # serialize collections of funcs as collections of func names
            return [fstr(func) for func in funcvec] if funcvec else []

        # mostly just a dump of the __dict__, some exceptions
        return dict(
            name=self.name,
            run=self.run,
            archive=self.archive,
            map_input_stream=fname(self.map_input_stream),
            map_function=fstr(self.map_function),
            reduce_function=fstr(self.reduce_function),
            keysets=self.params.keysets,
            parts_preprocess=self.params.parts_preprocess,
            parts_postprocess=self.params.parts_postprocess
        )
