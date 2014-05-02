import logging
import sys
import time
import pprint

from disco.core import Params
from disco.job import Job

from inferno.lib.archiver import Archiver
from inferno.lib.disco_ext import get_disco_handle
from inferno.lib.job_options import JobOptions
from inferno.lib.result import reduce_result
from datetime import datetime

log = logging.getLogger(__name__)

JOB_ARCHIVE = 'job.archive'
JOB_CLEANUP = 'job.cleanup'
JOB_BLOBS = 'job.blobs'
JOB_DONE = 'job.done'
JOB_PROCESS = 'job.process'
JOB_PROFILE = 'job.profile'
JOB_PURGE = 'job.purge'
JOB_RESULTS = 'job.results'
JOB_RUN = 'job.run'
JOB_START = 'job.start'
JOB_TAG = 'job.tag'
JOB_WAIT = 'job.wait'
JOB_ERROR = 'job.error'


class InfernoJob(object):
    def __init__(self, rule, settings, urls=None):
        self.job_options = JobOptions(rule, settings)
        self.rule = rule
        self.settings = settings
        rule_params = dict(rule.params.__dict__)
        self.disco, self.ddfs = get_disco_handle(rule_params.get('server', settings.get('server')))
        rule_params.update(settings)
        self.params = Params(**rule_params)
        self.urls = urls

        try:
            # attempt to allow for overriden worker class from settings file or rule
            if rule.worker:
                worker = rule.worker
            else:
                worker_mod, dot, worker_class = settings.get('worker').rpartition('.')
                mod = __import__(worker_mod, {}, {}, worker_mod)
                worker = getattr(mod, worker_class)()
            self.job = Job(name=rule.name,
                           master=self.disco.master,
                           worker=worker)
        except Exception as e:
            log.warn("Error instantiating worker: %s %s - loading default worker"
                     % (settings.get('worker'), e))
            self.job = Job(name=rule.name,
                           master=self.disco.master)
        self.full_job_id = None
        self.jobinfo = None
        self._notify(JOB_START)

    @property
    def job_name(self):
        return self.job.name

    @property
    def rule_name(self):
        return self.rule.qualified_name

    def start(self):
        # process the map-results option (ie. skip map phase and grab map results from job id/ddfs
        self.archiver = self._determine_job_blobs()
        job_blobs = self.archiver.job_blobs
        #print "BLOOBS: %s" % job_blobs
        self.start_time = time.time()
        if self.settings.get('just_query'):
            self.query()
            return None
        if self._enough_blobs(len(job_blobs)):
            if self.rule.rule_init_function:
                self.rule.rule_init_function(self.params)
            self.job.run(name=self.rule.name,
                         input=job_blobs,
                         map=self.rule.map_function,
                         reduce=self.rule.reduce_function,
                         params=self.params,
                         partitions=self.rule.partitions,
                         map_input_stream=self.rule.map_input_stream,
                         map_output_stream=self.rule.map_output_stream,
                         map_init=self.rule.map_init_function,
                         save=self.rule.save or self.rule.result_tag is not None,
                         scheduler=self.rule.scheduler,
                         combiner=self.rule.combiner_function,
                         reduce_output_stream=self.rule.reduce_output_stream,
                         sort=self.rule.sort,
                         sort_buffer_size=self.rule.sort_buffer_size,
                         profile=self.settings.get('profile'),
                         partition=self.rule.partition_function,
                         required_files=self.rule.required_files,
                         required_modules=self.rule.required_modules)
            # actual id is only assigned after starting the job
            self.full_job_id = self.job.name
            return self.job
        return None

    def query(self):
        log.info("Query information:")
        pprint.pprint({'source query': self.archiver.tags,
                       'tag results': self.archiver.tag_map,
                       'total_blobs': self.archiver.blob_count})

    def _safe_str(self, value):
        try:
            return str(value)
        except UnicodeEncodeError:
            return unicode(value).encode('utf-8')

    def wait(self):
        blob_count = self.archiver.blob_count
        log.info('Started job %s processing %i blobs',
                 self.job.name, blob_count)
        self._notify(JOB_WAIT)
        try:
            jobout = self.job.wait()
            log.info('Done waiting for job %s', self.job.name)
            self._profile(self.job)
            self._tag_results(self.job.name)
            if not self.settings.get('debug'):
                self._process_results(jobout, self.job.name)
            else:
                results = self._get_job_results(jobout)
                reduce_result(results)
            self._purge(self._safe_str(self.job.name))
        except Exception as e:
            log.error('Job %s failed', self.job.name)
            self._notify(JOB_ERROR)
            if self.rule.notify_on_fail:
                try:
                    from inferno.lib.notifications import send_mail
                    send_mail(job_id=self.job.name, job_fail=e,
                              mail_to=self.rule.notify_addresses,
                              mail_from=self.settings.get('mail_from'),
                              mail_server=self.settings.get('mail_server'))
                except Exception as e:
                    log.error('Job %s failed notification: %s', self.job.name, e, exc_info=sys.exc_info())
            raise
        else:
            if not self.settings.get('debug'):
                self._archive_tags(self.archiver)
            if self.rule.rule_cleanup:
                self._notify(JOB_CLEANUP)
                self.rule.rule_cleanup(self, )
            self._notify(JOB_DONE)
            if self.rule.notify_on_success:
                try:
                    from inferno.lib.notifications import send_mail
                    msg = "Job %s finished successfully." % self.job.name
                    send_mail(job_id=self.job.name, job_fail=msg,
                              mail_to=self.rule.notify_addresses,
                              mail_from=self.settings.get('mail_from'),
                              mail_server=self.settings.get('mail_server'))
                except Exception as e:
                    log.error('Job %s failed notification: %s', self.job.name, e, exc_info=sys.exc_info())
        log.info('Finished job %s', self.job.name)

    def _determine_job_blobs(self):
        self._notify(JOB_BLOBS)
        tags = self.job_options.tags
        urls = self.job_options.urls + self.urls if self.urls else self.job_options.urls
        if tags or urls:
            log.info('Processing input: %s...', (tags + urls)[:1000])
        else:
            log.info('No input available for %s.' % self.rule.name)
        archiver = Archiver(
            ddfs=self.ddfs,
            archive_prefix=self.rule.archive_tag_prefix,
            archive_mode=self.rule.archive,
            max_blobs=self.rule.max_blobs,
            tags=tags,
            urls=urls,
            newest_first=self.rule.newest_first,
        )
        return archiver

    def _get_job_results(self, jobout):
        if self.rule.result_iterator:
            self._notify(JOB_RESULTS)
            return self.rule.result_iterator(jobout)

    def _profile(self, job):
        if self.settings.get('profile'):
            self._notify(JOB_PROFILE)
            job.profile_stats().sort_stats('cumulative').print_stats()

    def _tag_results(self, job_name):
        if self.job_options.result_tag:
            self._notify(JOB_TAG)
            result_name = 'disco:job:results:%s' % job_name
            suffix = job_name
            # try to guess a better suffix (ie. the date)
            # sort the tags the job ran on, take the last part of the last tag
            # if that looks like a date, use it, otherwise use the job name
            if self.rule.result_tag_suffix:
                if str(self.rule.result_tag_suffix).lower() == "date":
                    suffix = str(datetime.now().date())
                else:
                    if len(self.job_options.tags):
                        tags = sorted(self.job_options.tags)
                        date = (tags[-1].split(':'))[-1]
                        if len(date) == 10 and '-' in date:
                            suffix = date
            tag_name = '%s:%s' % (self.job_options.result_tag, suffix)
            log.info('Tagging result: %s', tag_name)
            try:
                self.ddfs.tag(tag_name, list(self.ddfs.blobs(result_name)))
            except Exception:
                log.error('Error tagging result %s', tag_name)
                raise

    def _process_results(self, jobout, job_id):
        if self.rule.result_processor:
            self._notify(JOB_PROCESS)
            results = self._get_job_results(jobout)
            self.rule.result_processor(
                results, params=self.params, job_id=job_id)

    def _purge(self, job_name):
        if not self.settings.get('no_purge'):
            self._notify(JOB_PURGE)
            self.disco.purge(job_name)

    def _archive_tags(self, archiver):
        if archiver.archive_mode:
            self._notify(JOB_ARCHIVE)
            archiver.archive()

    def _notify(self, stage):
        # if we are daemon spawn, tell mommy where we are
        if self.full_job_id:
            log.info("Worker: %s stage= %s " % (self.full_job_id, stage))

    def _enough_blobs(self, blob_count):
        # Note that argument blob_count is the total number of tag blobs and urls.
        # To take urls into account, if no tag specified but urls are available,
        # let it run
        if len(self.job_options.tags) == 0:
            if blob_count:
                return True
            else:
                log.info('Skipping job %s: %d blobs required, has %d',
                         self.rule.name, self.rule.min_blobs, blob_count)
                return False

        if not blob_count or (blob_count < self.rule.min_blobs and not self.settings.get('force')):
            log.info('Skipping job %s: %d blobs required, has %d',
                     self.rule.name, self.rule.min_blobs, blob_count)
            return False
        return True

    def __str__(self):
        return '<InfernoJob for: %s>' % self.rule.name
