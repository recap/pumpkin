#!/usr/bin/env python
# encoding: utf-8



# $Source$#
from sys import version
import os
from setuptools import setup
import pumpkin


__version__ = pumpkin.VERSION

if version > '2.4' and version < '3.0':
    requires=['argparse', 'cmd',  'copy', 'fcntl', 'hashlib',  'imp',  'inspect', 'json',  'logging',  'matplotlib.pyplot',  'networkx',  'os', 'pyinotify', 'Queue', 're', 'signal', 'socket', 'struct', 'subprocess' , 'sys',  'time', 'atexit',  'thread',  'threading', 'time', 'uuid', 'zmq']

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='pumpkin',
    version=__version__,
    description='Distributed data processing and transformation network framework. ',
    long_description="""
    """,
    author='Reggie Cushing',
    author_email='R.S.Cushing@uva.nl',
    maintainer='DReggie Cushing',
    maintainer_email='delicious@echonest.com',
    url='https://github.com/recap/pumpkin',
    download_url='https://github.com/recap/pumpkin',
    package_dir={'pumpkin':'pumpkin'},
    packages=['pumpkin'],
    requires=requires
)

