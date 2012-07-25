import logging
import sys

from inferno.lib.lazy_property import lazy_property


log = logging.getLogger(__name__)


class Archiver(object):

    def __init__(self, ddfs, tags, urls=None, archive_prefix='processed', archive_mode=False, max_blobs=sys.maxint):
        self.tags = tags
        self.ddfs = ddfs
        self.max_blobs = max_blobs
        self.archive_mode = archive_mode
        self.archive_prefix = archive_prefix
        self.tag_map = self._build_tag_map(tags)
        self.urls = urls

    @lazy_property
    def blob_count(self):
        return len(self.job_blobs)

    @lazy_property
    def job_blobs(self):
        if self.urls:
            return self.urls
        else:
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
            blobs = self._labelled_blobs(tag)
            fresh_blobs = set(blobs.keys()) - set(archived_blobs.keys())
            if not fresh_blobs:
                continue

            fresh_blobs = [blobs[x] for x in fresh_blobs]
            tag_map[tag] = fresh_blobs[:(self.max_blobs - blob_count)]
            blob_count += len(fresh_blobs)
            if blob_count >= self.max_blobs:
                break
        return tag_map

    def _source_and_archived_sets(self, tags):
        source_tags = set()
        archived_blobs = dict()
        for prefix in tags:
            for tag in self.ddfs.list(prefix):
                source_tags.add(tag)
                if (self.archive_mode and
                    not tag.startswith(self.archive_prefix)):
                    archived_blobs.update(self._labelled_blobs(self._get_archive_name(tag)))
        source_tags = sorted(source_tags, reverse=True)
        return source_tags, archived_blobs

    def _labelled_blobs(self, tag):
        blobs = self.ddfs.blobs(tag)
        return dict(map(lambda blob: (blob[0].rsplit('/', 1)[1], blob), blobs))

    def _get_archive_name(self, tag):
        tag_parts = tag.split(':')
        tag_parts[0] = self.archive_prefix
        return ':'.join(tag_parts)
