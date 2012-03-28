import logging
import sys

from inferno.lib.lazy_property import lazy_property


log = logging.getLogger(__name__)


class Archiver(object):

    def __init__(self, ddfs, archive_prefix, archive_mode, max_blobs, tags):
        self.tags = tags
        self.ddfs = ddfs
        self.max_blobs = max_blobs
        self.archive_mode = archive_mode
        self.archive_prefix = archive_prefix
        self.tag_map = self._build_tag_map(tags)

    @lazy_property
    def blob_count(self):
        return len(self.job_blobs)

    @lazy_property
    def job_blobs(self):
        job_blobs = []
        for blobs in self.tag_map.itervalues():
            job_blobs += blobs
        return job_blobs

    def archive(self):
        if self.tag_map:
            try:
                self._archive_tags()
            except Exception as e:
                log.error('Archiving error: %s', e, exc_info=sys.exc_info())

    def _archive_tags(self):
        for tag, blobs in self.tag_map.iteritems():
            if tag.startswith(self.archive_prefix):
                log.info('Tag already starts with archive prefix: %s', tag)
            else:
                archive_name = self._get_archive_name(tag)
                self.ddfs.tag(archive_name, blobs)
                log.info('Archived %d blobs to %s', len(blobs), archive_name)

    def _build_tag_map(self, tags):
        tag_map = {}
        blob_count = 0
        source_tags, archived_blobs = self._source_and_archived_sets(tags)
        for tag in source_tags:
            blobs = [tuple(sorted(b)) for b in self.ddfs.blobs(tag)]
            blobs = set(blobs) - archived_blobs
            if blobs:
                tag_map[tag] = list(blobs)[:(self.max_blobs - blob_count)]
                blob_count += len(blobs)
                if blob_count >= self.max_blobs:
                    break
        return tag_map

    def _source_and_archived_sets(self, tags):
        source_tags = set()
        archived_blobs = set()
        for prefix in tags:
            for tag in self.ddfs.list(prefix):
                source_tags.add(tag)
                if (self.archive_mode and
                    not tag.startswith(self.archive_prefix)):
                    blobs = self.ddfs.blobs(self._get_archive_name(tag))
                    archived_blobs.update([tuple(sorted(b)) for b in blobs])
        source_tags = sorted(source_tags, reverse=True)
        return source_tags, archived_blobs

    def _get_archive_name(self, tag):
        tag_parts = tag.split(':')
        tag_parts[0] = self.archive_prefix
        return ':'.join(tag_parts)
