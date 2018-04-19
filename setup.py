#!/usr/bin/env python3

from distutils.core import setup
import os
import re


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', read('rdb_harvest/__init__.py'), re.MULTILINE).group(1)

setup(
    name='rdb-basel-harvester',
    packages=['rdb_harvest'],
    version=version,
    description='A simple package to harvest the data from the OAI-API of the Uni Basel Research Database.',
    author='Jonas Waeber',
    author_email='jonaswaeber@gmail.com',
    install_requires=['xmljson', 'sickle', 'simple-elastic'],
    url='https://github.com/UB-UNIBAS/rdb-harvest',
    download_url='https://github.com/UB-UNIBAS/rdb-harvest/archive/v' + version + '.tar.gz',
    keywords=['oai', 'research-data', 'basel'],
    classifiers=[],
    license='MIT'
)