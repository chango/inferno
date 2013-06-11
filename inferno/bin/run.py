import logging.config
import os
import shutil
import sys

import argparse
from disco.util import parse_dir
import yaml

from datetime import date
from time import mktime
from time import strptime

from setproctitle import setproctitle

from inferno.lib import __version__
from inferno.lib.disco_ext import get_disco_handle
from inferno.lib.job import InfernoJob
from inferno.lib.job_runner import execute_rule
from inferno.lib.lookup_rules import get_rules_by_name
from inferno.lib.settings import InfernoSettings


log = logging.getLogger(__name__)


def _get_options(argv):
    desc = 'Inferno: a python map/reduce library powered by disco.'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version='%s-%s' % ('inferno', __version__))

    parser.add_argument(
        "-s",
        "--server",
        dest="server",
        help="master disco server")

    parser.add_argument(
        "-e",
        "--settings",
        dest="settings_file",
        help="path to settings file")

    parser.add_argument(
        "-i",
        "--immediate-rule",
        dest="immediate_rule",
        help="execute <module>.<rule> immediately and exit")

    parser.add_argument(
        "-y",
        "--rules-directory",
        dest="rules_directory",
        help="directory to search for Inferno rules")

    parser.add_argument(
        "-q",
        "--just-query",
        dest="just_query",
        default=False,
        action="store_true",
        help="print out the blobs of the source query and \
        generated SQL, but don't execute the rule (only useful \
        for debugging rules in immediate mode)")

    parser.add_argument(
        "-f",
        "--force",
        dest="force",
        default=False,
        action="store_true",
        help="force processing of blobs")

    parser.add_argument(
        "-x",
        "--start-paused",
        dest="start_paused",
        default=False,
        action="store_true",
        help="start Inferno without starting any automatic rules (pause mode)")

    parser.add_argument(
        "-D",
        "--disco-debug",
        dest="disco_debug",
        default=False,
        action="store_true",
        help="debug map/reduce to disco console")

    parser.add_argument(
        "-d",
        "--debug",
        dest="debug",
        default=False,
        action="store_true",
        help="debug flag for inferno consumers")

    parser.add_argument(
        "-p",
        "--profile",
        dest="profile",
        default=False,
        action="store_true",
        help="output disco profiling data")

    parser.add_argument(
        "-t",
        "--source-tags",
        dest="source_tags",
        help="override the ddfs source tags")

    parser.add_argument(
        "-u",
        "--source-urls",
        dest="source_urls",
        help="override the source urls")

    parser.add_argument(
        "-r",
        "--result-tag",
        dest="result_tag",
        help="override the ddfs result tag")

    parser.add_argument(
        "-S",
        "--day-start",
        dest="day_start",
        help="override the start day for blobs")

    parser.add_argument(
        "-R",
        "--day-range",
        dest="day_range",
        type=int,
        help="override the day range for blobs")

    parser.add_argument(
        "-O",
        "--day-offset",
        dest="day_offset",
        type=int,
        help="override the days previous to start day for blobs")

    parser.add_argument(
        "-P",
        "--parameters",
        dest="parameters",
        default=[],
        action="append",
        help="additional rule parameters (in yaml)")

    parser.add_argument(
        "-l",
        "--parameter-file",
        dest="parameter_file",
        default=None,
        action="append",
        help="additional rule parameters (in a yaml file)")

    parser.add_argument(
        "--data-file",
        dest="data_file",
        default=None,
        help="arbitrary data file made available to job")

    parser.add_argument(
        "--example_rules",
        dest="example_rules",
        help="create example rules")

    parser.add_argument(
        "--process-results",
        dest="process_results",
        default=None,
        help="given a module.job_id, just run the result processor")

    parser.add_argument(
        "--process-map",
        dest="process_map",
        default=None,
        help="resume a job using the mapresults of supplied module.job_id")

    options = parser.parse_args(argv)

    if options.source_tags:
        options.source_tags = options.source_tags.split(',')

    if options.source_urls:
        qurls = options.source_urls.split(',')
        # the urls may be any kind of data, so commas may be quoted - account for this
        urls = []
        trail = ''
        for qurl in qurls:
            if qurl.endswith('\\'):
                trail += qurl[:-1] + ','
            else:
                if trail:
                    urls.append(trail + qurl)
                    trail = ''
                else:
                    urls.append(qurl)
        if trail:
            urls.append(trail)
        options.source_urls = urls

    if options.day_start:
        if not options.day_offset:
            options.day_offset = 0
        options.day_start = date.fromtimestamp(mktime(strptime(
            options.day_start, '%Y-%m-%d')))

    result = options.__dict__
    for parameter in options.parameters:
        result.update(yaml.load(parameter))

    if options.parameter_file:
        try:
            result.update(yaml.load(open(options.parameter_file, "r")))
        except Exception as e:
            message = "Error opening parameter file: %s %s"
            log.error(message, options.parameter_file, e)

    if options.data_file:
        try:
            data_file = open(options.data_file).readlines()
            result['data_file'] = data_file
        except Exception as e:
            message = "Could not open/process data file: %s %s"
            log.error(message, options.data_file, e)

    return result


def _get_settings(options):
    if options['settings_file']:
        settings = InfernoSettings(settings_file=options['settings_file'])
    else:
        settings = InfernoSettings()
    for key, value in options.iteritems():
        if value is not None or key not in settings:
            settings[key] = value
    return settings


def _setup_logging(settings):
    def _log_stdout(log):
        f = '%(asctime)s %(levelname)-5.5s %(process)d [%(name)s] %(message)s'
        logging.basicConfig(level=logging.DEBUG, format=f)

    log = logging.getLogger(__name__)
    if settings['immediate_rule']:
        _log_stdout(log)
    else:
        try:
            log_config = settings.get('log_config')
            logging.config.fileConfig(
                log_config, disable_existing_loggers=False)
        except Exception as e:
            log.error('Error setting up logging: %s' %  e)
            _log_stdout(log)

    log.info('Starting inferno-%s', __version__)
    log.info('Settings: \n%r', settings)


def main(argv=sys.argv):
    options = _get_options(argv[1:])
    settings = _get_settings(options)

    if options['example_rules']:
        try:
            os.mkdir(options['example_rules'])
            here = os.path.dirname(__file__)
            src_dir = os.path.join(here, '..', 'example_rules')
            src_dir = os.path.abspath(src_dir)
            dst_dir = os.path.abspath(options['example_rules'])
            for name in os.listdir(src_dir):
                if name.endswith('.py'):
                    src = os.path.join(src_dir, name)
                    dst = os.path.join(dst_dir, name)
                    shutil.copy(src, dst)
            print '\n\tCreated example rules dir:\n\n\t\t%s' % dst_dir
            for name in os.listdir(dst_dir):
                print '\t\t\t', name
        except Exception as e:
            print 'Error creating example rules dir %r' % (e)
        finally:
            return

    _setup_logging(settings)

    for path in settings.get('extra_python_paths'):
        sys.path.insert(0, path)

    if options['process_results']:
        settings['no_purge'] = True
        rules_dir = options.get('rules_directory')
        if not rules_dir:
            rules_dir = settings.get('rules_directory')
        try:
            rule_name = options['process_results'].split('@')[0]
            job_name = options['process_results'].split('.')[1]
            rule = get_rules_by_name(rule_name, rules_dir, immediate=True)[0]
            job = InfernoJob(rule, settings)
            status, results = job.disco.results(job_name)
            if status == 'ready':
                rule.result_processor(rule.result_iterator(results), params=job.params, job_id=job_name)
        except Exception as e:
            import traceback
            trace = traceback.format_exc(15)
            log.error(trace)
            log.error("Error processing results for job: %s %s" % (options['process_results'], e))
            raise e
    elif options['process_map']:
        settings['no_purge'] = True
        rules_dir = options.get('rules_directory')
        if not rules_dir:
            rules_dir = settings.get('rules_directory')
        try:
            rule_name = options['process_map'].split('@')[0]
            job_name = options['process_map'].split('.')[1]
            rule = get_rules_by_name(rule_name, rules_dir, immediate=True)[0]
            rule.map_function = None
            rule.source_tags = []
            disco, ddfs = get_disco_handle(settings.get('server'))
            rule.source_urls = disco.mapresults(job_name)
            job = InfernoJob(rule, settings)
            if job.start():
                job.wait()
        except Exception as e:
            import traceback
            trace = traceback.format_exc(15)
            log.error(trace)
            log.error("Error processing map results for job: %s %s" % (options['process_map'], e))
            raise e
    elif options['immediate_rule']:
        # run inferno in 'immediate' mode
        settings['no_purge'] = True
        setproctitle('inferno - immediate.%s' % options['immediate_rule'])
        immed_rule = settings.get('immediate_rule')
        rules_dir = settings.get('rules_directory')
        rules = get_rules_by_name(immed_rule, rules_dir, immediate=True)
        try:
            for rule in rules:
                execute_rule(rule, settings)
        except Exception as e:
            log.info('Job failed: %s' % str(e))
    else:
        # run inferno in 'daemon' mode
        from inferno.lib.daemon import InfernoDaemon
        setproctitle('inferno - master')
        InfernoDaemon(settings).start()

if __name__ == '__main__':
    main()
