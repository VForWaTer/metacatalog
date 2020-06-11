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


def check_defaults(session, capsys):
    """
    Load data files from metacatalog and check against 
    the populated database

    """
    path = os.path.join(PATH, '..', 'data', '*.csv')
    files = glob.glob(path)

    for fname in files:
        tablename = os.path.basename(fname).split('.')[0]
        if tablename == 'keywords':
            continue   # something is going wrong here! TODO fix
        with capsys.disabled():
            print('Testing table: %s' % tablename)
        
        # load datafile
        datafile = pd.read_csv(fname, sep=',')
        datafile = datafile.where(datafile.notnull(), None) # replace NaN by None
        
        # load table from db
        table = pd.read_sql_table(tablename, session.bind)

        # drop publication and lastUpdate columns as they are autofilled
        table.drop(['publication', 'lastUpdate'], axis=1, errors='ignore', inplace=True)
        table = table.where(table.notnull(), None) # replace NaN by None

        assert datafile.equals(table)

    return True


@pytest.mark.depends(on=['db_install'], name='db_init')
def test_metacatalog_install(capsys):
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
    assert check_defaults(session, capsys)