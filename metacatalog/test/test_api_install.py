import pytest
import os
import glob
import pandas as pd

from metacatalog import api
from ._util import connect, PATH


def create_tables(session):
    """
    Install all tables 

    """
    # test
    api.create_tables(session)
    return True


def populate_defaults(session):
    """
    Populate default values

    """
    api.populate_defaults(session)
    return True


def check_defaults(session):
    """
    Load data files from metacatalog and check against 
    the populated database

    """
    path = os.path.join(PATH, '..', 'data', '*.csv')
    files = glob.glob(path)

    for fname in files:
        datafile = pd.read_csv(fname, sep=',')
        tablename = os.path.basename(fname).split('.')[0]
        table = pd.read_sql_table(tablename, session.bind)

        assert datafile.equals(table)

    return True


@pytest.mark.depends(on=['db_install'])
def test_metacatalog_install():
    """
    Depends on Postgis install.
    Runs tests on creating tables and populating defaults 
    using the Python api
    """
    # connect to db
    session = connect(mode='session')
    
    # run single tests
    assert create_tables(session)
    assert populate_defaults(session)
    assert check_defaults(session)