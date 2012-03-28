import os
import re
import sys

from setuptools import setup, find_packages


if sys.version_info[:2] < (2, 6):
    raise RuntimeError('Requires Python 2.6 or better')

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
info = open(os.path.join(here, 'inferno', 'lib', '__init__.py')).read()
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(info).group(1)

install_requires = [
    'argparse',
    'pyyaml',
    'setproctitle',
    'tornado',
    'ujson',
]

tests_require = install_requires + [
    'nose',
    'mock']

setup(
    name='inferno',
    version=VERSION,
    description=('Inferno: a python map/reduce platform powered by disco.'),
    long_description=README,
    keywords='inferno discoproject',
    author='Chango Inc.',
    author_email='dev@chango.com',
    url='http://chango.com',
    license='MIT License',
    packages=find_packages(exclude=['test']),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite='nose.collector',
    entry_points="""
    [console_scripts]
    inferno=inferno.bin.run:main
    """)
