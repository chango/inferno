from datetime import date

from mock import Mock
from nose.tools import eq_
from nose.tools import ok_

from inferno.lib.job import JOB_ARCHIVE
from inferno.lib.job import JOB_BLOBS
from inferno.lib.job import JOB_PROCESS
from inferno.lib.job import JOB_PROFILE
from inferno.lib.job import JOB_PURGE
from inferno.lib.job import JOB_RESULTS
from inferno.lib.job import JOB_TAG
from inferno.lib.job import InfernoJob
from inferno.lib.rule import InfernoRule
from inferno.lib.settings import InfernoSettings

from test.mock.disco import DDFS
from test.mock.disco import Disco


class TestJob(object):

    MAX_BLOBS = 1000
    ARCHIVE_PREFIX = 'archived'

    def setUp(self):
        settings = InfernoSettings(day_range=2, day_start=date(2011, 11, 12))
        rule = InfernoRule(
            archive=True,
            max_blobs=self.MAX_BLOBS,
            name='some_rule_name',
            archive_tag_prefix='archived',
            source_tags=['incoming:data:chunk'])
        self.job = InfernoJob(rule, settings)
        self.job.disco = Disco()
        self.job.ddfs = DDFS()

    def test_start_not_enough_blobs(self):
        self.job.rule.min_blobs = 1000
        job = self.job.start()
        eq_(job, None)

    def test_determine_job_blobs(self):
        expected_tags = [
            'incoming:data:chunk:2011-11-12',
            'incoming:data:chunk:2011-11-11']
        expected_blobs = [
            ('/b12.1', '/b12.2', '/b12.3'),
            ('/b11.1', '/b11.2', '/b11.3')]
        archiver = self.job._determine_job_blobs()

        try:
            # check that the archiver was created correctly
            eq_(archiver.max_blobs, self.MAX_BLOBS)
            eq_(archiver.archive_mode, True)
            eq_(archiver.archive_prefix, self.ARCHIVE_PREFIX)

            # check that it found the correct tags and blobs
            eq_(archiver.tags, expected_tags)
            eq_(archiver.job_blobs, expected_blobs)

        except Exception as e:
            raise e

    def test_archive_tags(self):
        # there should be no archived tags before calling archive
        archive_prefix = 'archived:data:chunk'
        archiver = self.job._determine_job_blobs()
        archived = archiver.ddfs.list(archive_prefix)
        eq_(archived, [])

        # should not archive & change state since archive mode is false
        archiver.archive_mode = False
        self.job._archive_tags(archiver)
        archived = archiver.ddfs.list(archive_prefix)
        eq_(archived, [])

        # should archive & change state since archive mode is true
        archiver.archive_mode = True
        self.job._archive_tags(archiver)
        archived = archiver.ddfs.list(archive_prefix)
        expected = [
            'archived:data:chunk:2011-11-11',
            'archived:data:chunk:2011-11-12']
        eq_(archived, expected)

    def test_get_job_results_no_results(self):
        urls = []
        actual = self.job._get_job_results(urls)
        try:
            eq_(list(actual), [])
        except Exception as e:
            raise e

    def test_process_results_no_results(self):
        results = []
        self.job._process_results(results, 'some_job_id')
        #eq_(self.job.current_stage, JOB_PROCESS)

    def test_purge(self):
        # should purge if there's no 'no_purge' setting
        ok_('no_purge' not in self.job.settings)
        self.job._purge('some_job_name')

        # should not purge & change state
        self.job._purge('some_job_name')

        # should purge & change state
        self.job.settings['no_purge'] = False
        self.job._purge('some_job_name')

    def test_profile(self):
        # should not profile if there's no 'profile' setting
        ok_('profile' not in self.job.settings)
        self.job._profile(Mock())

        # should not profile & change state
        self.job.settings['profle'] = False
        self.job._profile(Mock())

        # should profile & change state
        self.job.settings['profile'] = True
        self.job._profile(Mock())

    def test_tag_results(self):
        # should not tag results & change state
        self.job.settings['result_tag'] = None
        self.job._tag_results('some_job_name')

        # should tag results & change state
        before = self.job.ddfs.list('some_result_tag')
        eq_(before, [])

    def test_enough_blobs(self):
        # equal
        self.job.rule.min_blobs = 100
        self.job.settings['force'] = False
        eq_(self.job._enough_blobs(blob_count=100), True)

        # more than
        self.job.rule.min_blobs = 100
        self.job.settings['force'] = False
        eq_(self.job._enough_blobs(blob_count=101), True)

        # less than
        self.job.rule.min_blobs = 100
        self.job.settings['force'] = False
        eq_(self.job._enough_blobs(blob_count=99), False)

        # less than, but force mode is enabled
        self.job.rule.min_blobs = 100
        self.job.settings['force'] = True
        eq_(self.job._enough_blobs(blob_count=99), True)

    def test_str(self):
        eq_(str(self.job), '<InfernoJob for: some_rule_name>')
