"""
Install Database
----------------

This script is a prerequisite for all other e2e-tests 
as it will install the PostgreSQL database and create 
the postgis extension. It needs PostreSQL server and 
postgis to be installed on the host machine.

This 'test' will use the given POSTGRES_DB, which 
defaults to ``postgres`` to connect and create a 
database. Please make sure that the user has permission.
The new database name will be random and exported 
into a file called 'DBNAME' in this directory. 
It will also export the name to the current environment.

"""
import os
import random
import string
import pytest

from ._util import connect

PATH = os.path.abspath(os.path.dirname(__file__))


def test_database_install():
    """
    Install a new instance of metacatalog.
    This test is a prerequisite for all other tests.
    """
    # make up a random name
    name = 'test_%s' % ''.join([random.choice(string.ascii_lowercase) for _ in range(8)])
    # get engine
    engine = connect(mode='engine')

    with engine.connect() as con:
        con.execute('commit')
        con.execute('CREATE DATABASE %s' % name)

    # export the db name
    os.putenv("POSTGRES_TESTDB", name)
    with open(os.path.join(PATH, 'DBNAME'), 'w') as f:
        f.write(name)


@pytest.mark.depends(on=['test_database_install'], name="db_install")
def test_postgis_install(capsys):
    """
    Installs PostGIS extension into the newly created database
    """
    engine = connect(mode='engine')
    with capsys.disabled():
        print(engine)


    # create postgis and check 
    with engine.connect() as con:
        con.execute('CREATE EXTENSION postgis;')
        con.execute('commit')
        res = con.execute('SELECT PostGIS_full_version();').scalar()
    
    # get the version
    assert 'POSTGIS=' in res

    with capsys.disabled():
        print(res)

