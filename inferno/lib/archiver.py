import logging, sys

log = logging.getLogger(__name__)


class Archiver(object):

    def __init__(self,
                 ddfs,
                 tags,
                 urls=None,
                 archive_prefix='processed',
                 archive_mode=False,
                 max_blobs=None,
                 newest_first=True,
                 days=0):
        self.tags = tags
        self.ddfs = ddfs
        self.archive_mode = archive_mode

        if not archive_mode and not max_blobs:
            self.max_blobs = sys.maxint
        elif archive_mode and not max_blobs:
            self.max_blobs = 500
        elif max_blobs:
            self.max_blobs = max_blobs
        else:
            self.max_blobs = sys.maxint

        self.archive_prefix = archive_prefix
        self.newest_first = newest_first
        self.urls = urls
        self.days = days

        self._outgoing_blobs = None
        self._processed_blobs = None
        self._tag_map = list()
        self._job_blobs = list()
        self._blob_count = 0


    @property
    def processed_blobs(self):
        if not self._processed_blobs:
            blobs = set()
            days = 0
            for tag in self.tags:
                processed_prefix = self._get_archive_name(tag)
                for processed_tag in self.tag_list(processed_prefix):
                    days += 1
                    for blob in self.ddfs.blobs(processed_tag):
                        blobs.add(self.ddfs.blob_name(blob[0]))
                    if self.days != 0 and days == self.days:
                        break
                days = 0
            self._processed_blobs = blobs
        return self._processed_blobs


    @property
    def outgoing_blobs(self):
        if not self._outgoing_blobs:
            self._processed_blobs = self.processed_blobs
            fresh_blobs = dict()
            added_tags = set()
            days = 0
            for incoming_prefix in self.tags:
                fresh_blobs[incoming_prefix] = dict()
                for incoming_tag in self.tag_list(incoming_prefix):
                    for blob in self.ddfs.blobs(incoming_tag):
                        if self.ddfs.blob_name(blob[0]) not in self.processed_blobs:
                            if incoming_tag not in fresh_blobs[incoming_prefix]:
                                fresh_blobs[incoming_prefix][incoming_tag] = list()
                            fresh_blobs[incoming_prefix][incoming_tag].append(sorted(blob))
                        if sum(map(len, fresh_blobs[incoming_prefix].values())) == self.max_blobs:
                            added_tags.add(incoming_prefix)
                            break
                    days += 1
                    if self.days != 0 and days == self.days:
                        break
                    if incoming_prefix in added_tags:
                        break
                days = 0
            self._outgoing_blobs = fresh_blobs
        return self._outgoing_blobs


    @property
    def blob_count(self):
        if not self._blob_count:
            for tags in self.outgoing_blobs.values():
                for blobs in tags.values():
                    self._blob_count += len(blobs)
        return self._blob_count


    @property
    def job_blobs(self):
        if not self._job_blobs:
            if self.archive_mode:
                for prefix, tags in self.outgoing_blobs.iteritems():
                    for tag, blobs in tags.iteritems():
                        self._job_blobs += [blob for blob in blobs]
            else:
                tag_blobs = dict()
                for prefix in self.tags:
                    if prefix not in tag_blobs:
                        tag_blobs[prefix] = 0
                    for tag in self.tag_list(prefix):
                        for blobs in self.ddfs.blobs(tag):
                            self._job_blobs.append(blobs)
                            tag_blobs[prefix] += 1
                            if tag_blobs[prefix] == self.max_blobs:
                                break
                        if tag_blobs.get(prefix,0) == self.max_blobs:
                            break
                self._blob_count = len(self._job_blobs)
                self._tag_map = self.tags
            self._job_blobs += self.urls
            self._blob_count += len(self.urls)
        return self._job_blobs


    @property
    def tag_map(self):
        if not self._tag_map:
            for prefix, tags in self.outgoing_blobs.iteritems():
                self._tag_map.append(tags.keys())
        return self._tag_map


    def tag_list(self, prefix):
        return sorted(self.ddfs.list(prefix), reverse=self.newest_first)


    def archive(self):
        if self.outgoing_blobs and self.archive_mode:
            for prefix, tags in self.outgoing_blobs.iteritems():
                for tag, blobs in tags.iteritems():
                    if tag.startswith(self.archive_prefix):
                        log.info('Tag already starts with archive prefix: %s', tag)
                    else:
                        archive_name = self._get_archive_name(tag)
                        self.ddfs.tag(archive_name, blobs)
                        log.info('Archived %d blobs to %s', len(blobs), archive_name)


    def _get_archive_name(self, tag):
        tag_parts = tag.split(':')
        tag_parts[0] = self.archive_prefix
        return ':'.join(tag_parts)
