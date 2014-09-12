import logging
import sys

from inferno.lib.lazy_property import lazy_property


log = logging.getLogger(__name__)


class Archiver(object):

    def __init__(self,
                 ddfs,
                 tags,
                 urls=None,
                 archive_prefix='processed',
                 archive_mode=False,
                 nuke_mode=False,
                 max_blobs=sys.maxint,
                 newest_first=True):
        self.tags = tags
        self.ddfs = ddfs
        self.max_blobs = max_blobs
        self.archive_mode = archive_mode
        self.nuke_mode = nuke_mode
        self.archive_prefix = archive_prefix
        self.newest_first = newest_first
        self.tag_map = self._build_tag_map(tags)
        self.urls = urls if urls is not None else []

    @property
    def blob_count(self):
        return len(self.job_blobs)

    @lazy_property
    def job_blobs(self):
        # job_blobs consist of blobs of tags and urls
        job_blobs = []
        for blobs in self.tag_map.itervalues():
            job_blobs += blobs
        job_blobs += self.urls
        return job_blobs

    def archive(self):
        if self.tag_map:
            try:
                self._archive_tags()
            except Exception as e:
                log.error('Archiving error: %s', e, exc_info=sys.exc_info())

    def nuke(self):
        try:
            self.ddfs.delete(self.tags)
            log.info('Deleted %s .', self.tags)
        except Exception as e:
                log.error('Deleting error: %s', e, exc_info=sys.exc_info())

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
            warned = False
            for blob in self.ddfs.blobs(tag):
                #normalized_blob = tuple(sorted(blob))
                if len(blob):
                    blob_name = self.get_blob_name(blob[0])
                    if blob_name not in archived_blobs:
                        incoming_blobs = tag_map.setdefault(tag, [])
                        if blob_count == self.max_blobs:
                            return tag_map
                        incoming_blobs.append(blob)
                        blob_count += 1
                else:
                    if not warned:
                        log.warning("Tag %s contains *empty* blobs.  Skipping..." % tag)
                        warned = True

                assert blob_count <= self.max_blobs
                if blob_count == self.max_blobs:
                    return tag_map
        return tag_map

    def _source_and_archived_sets(self, tags):
        source_tags = set()
        archived_blobs = set()
        for prefix in tags:
            for tag in self.ddfs.list(prefix):
                source_tags.add(tag)
                archive_tag = self._get_archive_name(tag)
                if self.archive_mode and not tag.startswith(self.archive_prefix):
                    archived_blobs.update(self._normalized_blobs(archive_tag))
        source_tags = sorted(source_tags, reverse=self.newest_first)
        return source_tags, archived_blobs

    def get_blob_name(self, blob):
        try:
            return blob.rsplit('/', 1)[1]
        except:
            return []

    def _normalized_blobs(self, tag):
        rval = set()
        for blob in self.ddfs.blobs(tag):
            if len(blob) == 0:
                log.error("Disco returned an empty list for a blob in tag %s" % tag)
                continue
            try:
                rval.add(self.get_blob_name(blob[0]))
            except:
                log.error("Error getting blob name: %s" % blob)
        return rval

    def _get_archive_name(self, tag):
        tag_parts = tag.split(':')
        tag_parts[0] = self.archive_prefix
        return ':'.join(tag_parts)
