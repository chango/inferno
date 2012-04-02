import logging.config
import os
import shutil
import sys

import argparse
import yaml

from datetime import date
from time import mktime
from time import strptime

from setproctitle import setproctitle

from inferno.lib import __version__
from inferno.lib.job_factory import JobFactory
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

    options = parser.parse_args(argv)

    if options.source_tags:
        options.source_tags = options.source_tags.split(',')

    if options.day_start:
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
        logging.basicConfig(level=logging.INFO, format=f)

    log = logging.getLogger(__name__)
    if settings['immediate_rule']:
        _log_stdout(log)
    else:
        try:
            log_config = settings.get('log_config')
            logging.config.fileConfig(
                log_config, disable_existing_loggers=False)
        except Exception as e:
            log.error('Error setting up logging [%s]: %s' % (log_config, e))
            _log_stdout(log)

    log.info('Starting inferno-%s', __version__)
    log.debug('Settings: \n%r', settings)


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
            print '\n\tCreated example rules dir:\n\n\t\t%s' % (dst_dir)
            for name in os.listdir(dst_dir):
                print '\t\t\t', name
        except Exception as e:
            print 'Error creating example rules dir %r' % (e)
        finally:
            return

    _setup_logging(settings)

    for path in settings.get('extra_python_paths'):
        sys.path.insert(0, path)

    if options['immediate_rule']:
        # run inferno in 'immediate' mode
        setproctitle('inferno - immediate.%s' % options['immediate_rule'])
        JobFactory.execute_immediate_rule(settings)
    else:
        # run inferno in 'daemon' mode
        from inferno.lib.daemon import InfernoDaemon
        setproctitle('inferno - master')
        InfernoDaemon(settings).start()
