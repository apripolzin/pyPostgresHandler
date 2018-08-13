from setuptools import setup

setup(
    name='dbh',
    version='1.0.0',
    author='Alexey Pripolzin',
    packages=['dbh', ],
    description='Simple postgres database handler',
    long_description='Simple postgres database handler',
    install_requires=["psycopg2", ],
    data_files=[('/etc/dbh/', ['etc/dbh/db_connection.conf', ]), ]
)


