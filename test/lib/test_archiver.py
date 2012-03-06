from nose.tools import eq_
from nose.tools import ok_

from inferno.lib.archiver import Archiver

from test.mock.disco import DDFS


class TestArchiver(object):

    def _setup(self, tags=()):
        self.archiver = Archiver(
            ddfs=DDFS(),
            archive_prefix='processed',
            archive_mode=True,
            max_blobs=100,
            tags=tags)

    def test_get_archive_name(self):
        self._setup()
        tag = 'incoming:data:chunk:2012-12-01'
        actual = self.archiver._get_archive_name(tag)
        eq_(actual, 'processed:data:chunk:2012-12-01')

    def test_blob_count(self):
        self._setup()
        self.archiver.tag_map = self.fake_tag_map
        eq_(self.archiver.blob_count, 5)

    def test_job_blobs(self):
        self._setup()
        self.archiver.tag_map = self.fake_tag_map
        expected = [
            ('blob1.a', 'blob1.b', 'blob1.c'),
            ('blob2.a', 'blob2.b', 'blob2.c'),
            ('blob3.a', 'blob3.b', 'blob3.c'),
            ('blob4.a', 'blob4.b', 'blob4.c'),
            ('blob5.a', 'blob5.b', 'blob5.c')]
        eq_(self.archiver.job_blobs, expected)

    @property
    def fake_tag_map(self):
        return {
            'tag1': [
                ('blob1.a', 'blob1.b', 'blob1.c'),
                ('blob2.a', 'blob2.b', 'blob2.c')],
            'tag2': [
                ('blob3.a', 'blob3.b', 'blob3.c'),
                ('blob4.a', 'blob4.b', 'blob4.c'),
                ('blob5.a', 'blob5.b', 'blob5.c')]}

    def test_archive(self):
        incoming_tag = 'incoming:data:chunk:2011-11-13'
        archived_tag = 'processed:data:chunk:2011-11-13'

        # no archived tags before the archive call
        self._setup(tags=[incoming_tag])
        eq_([incoming_tag], self.archiver.ddfs.list(incoming_tag))
        eq_([], self.archiver.ddfs.list(archived_tag))

        # one archived tag after the archive call
        self.archiver.archive()
        eq_([incoming_tag], self.archiver.ddfs.list(incoming_tag))
        eq_([archived_tag], self.archiver.ddfs.list(archived_tag))

        # incoming and archived tags point to the same blobs
        expected_blobs = [
            ('b13.1', 'b13.2', 'b13.3'),
            ('b13.1.a', 'b13.2.a', 'b13.3.a')]
        incoming_blobs = self.archiver.ddfs.blobs(incoming_tag)
        archived_blobs = self.archiver.ddfs.blobs(archived_tag)
        eq_(incoming_blobs, expected_blobs)
        eq_(archived_blobs, expected_blobs)


class TestBuildTagMap(object):

    def _setup(self, archive_mode=True, max_blobs=100, archive_some=False):
        ddfs = DDFS()
        if archive_some:
            blobs = ('b13.1', 'b13.2', 'b13.3')
            ddfs.ddfs['processed:data:chunk:2011-11-13'] = [blobs]
        self.archiver = Archiver(
            ddfs=ddfs,
            archive_prefix='processed',
            archive_mode=archive_mode,
            max_blobs=max_blobs,
            tags=['incoming:data:chunk'])

    def test_tag_map(self):
        self._setup()
        self._assert_full_tag_map()

    def test_partially_archived(self):
        self._setup(archive_some=True)
        self._assert_tag_map_minus_processed()

    def test_archive_mode_off(self):
        self._setup(archive_some=True, archive_mode=False)
        self._assert_full_tag_map()

    def test_max_blobs_zero(self):
        expected = {'incoming:data:chunk:2011-11-14': []}
        self._assert_max_blobs(expected, max_blobs=0)

    def test_max_blobs_some_of_one_tag(self):
        expected = {
            'incoming:data:chunk:2011-11-14': [
                ('b14.1', 'b14.2', 'b14.3'),
                ('b14.1.a', 'b14.2.a', 'b14.3.a')]}
        self._assert_max_blobs(expected, max_blobs=2)

    def test_max_blobs_exactly_one_tag(self):
        expected = {
            'incoming:data:chunk:2011-11-14': [
                ('b14.1', 'b14.2', 'b14.3'),
                ('b14.1.a', 'b14.2.a', 'b14.3.a'),
                ('b14.1.b', 'b14.2.b', 'b14.3.b')]}
        self._assert_max_blobs(expected, max_blobs=3)

    def test_max_blobs_more_than_one_tag(self):
        expected = {
            'incoming:data:chunk:2011-11-13': [
                ('b13.1.a', 'b13.2.a', 'b13.3.a')],
            'incoming:data:chunk:2011-11-14': [
                ('b14.1', 'b14.2', 'b14.3'),
                ('b14.1.a', 'b14.2.a', 'b14.3.a'),
                ('b14.1.b', 'b14.2.b', 'b14.3.b')]}
        self._assert_max_blobs(expected, max_blobs=4)

    def _assert_max_blobs(self, expected, max_blobs):
        self._setup(archive_some=True, max_blobs=max_blobs)
        self._compare_blobs(self.archiver.tag_map, expected)
        eq_(self.archiver.blob_count, max_blobs)

    def _assert_full_tag_map(self):
        expected = {
            'incoming:data:chunk:2011-11-11': [
                ('b11.1', 'b11.2', 'b11.3')],
            'incoming:data:chunk:2011-11-12': [
                ('b12.1', 'b12.2', 'b12.3')],
            'incoming:data:chunk:2011-11-13': [
                ('b13.1', 'b13.2', 'b13.3'),
                ('b13.1.a', 'b13.2.a', 'b13.3.a')],
            'incoming:data:chunk:2011-11-14': [
                ('b14.1', 'b14.2', 'b14.3'),
                ('b14.1.a', 'b14.2.a', 'b14.3.a'),
                ('b14.1.b', 'b14.2.b', 'b14.3.b')]}
        self._compare_blobs(self.archiver.tag_map, expected)
        eq_(self.archiver.blob_count, 7)

    def _assert_tag_map_minus_processed(self):
        expected = {
            'incoming:data:chunk:2011-11-11': [
                ('b11.1', 'b11.2', 'b11.3')],
            'incoming:data:chunk:2011-11-12': [
                ('b12.1', 'b12.2', 'b12.3')],
            'incoming:data:chunk:2011-11-13': [
                ('b13.1.a', 'b13.2.a', 'b13.3.a')],
            'incoming:data:chunk:2011-11-14': [
                ('b14.1', 'b14.2', 'b14.3'),
                ('b14.1.a', 'b14.2.a', 'b14.3.a'),
                ('b14.1.b', 'b14.2.b', 'b14.3.b')]}
        self._compare_blobs(self.archiver.tag_map, expected)
        eq_(self.archiver.blob_count, 6)

    def _compare_blobs(self, tag_map, expected):
        eq_(len(expected), len(tag_map))
        for tag, blob in expected.iteritems():
            ok_(tag in tag_map)
            tag_map_blob = tag_map[tag]
            eq_(len(tag_map_blob), len(blob))
            for replica in blob:
                ok_(replica in tag_map_blob)
