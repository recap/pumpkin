#!/usr/bin/env python
# encoding: utf-8



# $Source$#
from sys import version
import os
from setuptools import setup

__version__ = 1.0

if version > '2.4' and version < '3.0':
    requires=['ujson','pyinotify','tftpy','networkx','zmq','pika','pystun','netaddr','pyftpdlib']

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
    install_requires=requires
)

