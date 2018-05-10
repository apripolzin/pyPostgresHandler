#!/usr/bin/env python3

from setuptools import setup

setup(
    name='dbh',
    version='1.0.0',
    author='Alexey Pripolzin',
    packages=['dbh', ],
    description='Simple postgres database handler',
    long_description='Simple postgres database handler',
    install_requires=["psycopg2", ],
    data_files=[('/opt/ankf/etc/', ['opt/ankf/etc/db_connection.conf', ]), ]
)
