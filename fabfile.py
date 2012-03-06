import os
import re
import sys

from fabric.api import sudo, run, local, env, cd
from fabric.decorators import runs_once


_sudo = sudo
here = os.path.abspath(os.path.dirname(__file__))
env.user = 'chango'
env.roledefs = {
    'dancefloor': ['dancefloor01', 'cronkite01'],
    'test_dashboard': ['dev-dashboard01'],
}

PROJECT_NAME = 'inferno'
RELEASE_DIR = '/etc/inferno/minipop'
VERSION_PATH = os.path.join(here, 'inferno', 'lib', '__init__.py')


def sudo(*args, **kwargs):
    kwargs.update({'shell': False})
    _sudo(*args, **kwargs)


@runs_once
def prepare_deploy():
    print "Please enter a deploy log message:"
    response = sys.stdin.readline().strip()
    if not response:
        print "Aborted. No log message"
        exit(1)
    tag_name = bump_version('patch', response)
    return tag_name


def deploy(tag_name=None):
    if not tag_name:
        tag_name = prepare_deploy()
    update_code(tag_name)


def update_code(tag_name):
    with cd(RELEASE_DIR):
        print "Deploying tag %s..." % tag_name
        run("hg pull")
        run("hg up -C %s" % tag_name)
        run("python setup.py install")


def bump_version(release_type='patch', message='bumping version'):
    new_version = get_new_version(release_type)
    with open(VERSION_PATH, 'w') as lib_info:
        lib_info.write("__version__ = %r\n" % (new_version))
    local('hg commit -m "VERSION: %s"' % (new_version))
    local('hg push')
    tag_name = 'release-%s-%s' % (PROJECT_NAME, new_version)
    local('hg tag -m "TAG [%s]: %s" %s' % (tag_name, message, tag_name))
    local('hg push')
    return tag_name


def get_current_version():
    with open(VERSION_PATH) as lib_info:
        pattern = r".*__version__ = '(.*?)'"
        version = re.compile(pattern, re.S).match(lib_info.read()).group(1)
    return version


def get_new_version(release_type='patch'):
    current_version = get_current_version()
    print 'current version: %r' % current_version
    major, minor, patch = map(int, current_version.split('.'))
    if release_type == 'major':
        major += 1
        minor = 0
        patch = 0
    if release_type == 'minor':
        minor += 1
        patch = 0
    if release_type == 'patch':
        patch += 1
    new_version = '.'.join(map(str, [major, minor, patch]))
    print 'new version: %r' % new_version
    return new_version
