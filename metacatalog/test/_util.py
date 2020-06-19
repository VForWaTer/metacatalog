import os
import io

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

PATH = os.path.abspath(os.path.dirname(__file__))
DBFILE = os.path.abspath(os.path.join(PATH, 'DBNAME'))

def connect(mode='string'):
    # first read the TESTDB name from file if exists
    if os.path.exists(DBFILE):
        with open(DBFILE, 'r') as f:
            DB = f.read()
    else:
        DB = 'postgres'
    
    # read environment variables or use defaults
    SETTINGS = dict(
        HOST=os.environ.get('POSTGRES_HOST', 'localhost'),
        DB=os.environ.get('POSTGRES_TESTDB', DB),
        USER=os.environ.get('POSTGRES_USER', 'postgres'),
        PWD=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
        PORT=os.environ.get('POSTGRES_PORT', '5432')
    )

    # build db uri
    URI = 'postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}'.format(**SETTINGS)
    
    # return mode
    if mode.lower() == 'string':
        return URI
    elif mode.lower() == 'dict':
        return SETTINGS
    elif mode.lower() == 'engine':
        return create_engine(URI, poolclass=NullPool)
    elif mode.lower() == 'session':
        Session = sessionmaker(bind=create_engine(URI, poolclass=NullPool))
        return Session()


def read_to_df(s):
    """
    Read a csv formatted string into a file-like-object 
    and then open using pandas and return dataframe

    This is used to open some data to test against
    """
    # create file-like object
    fs = io.StringIO()
    fs.write(s)
    fs.seek(0)

    # read
    return pd.read_csv(fs)


def cleanup(drop_db=False):
    """
    Right now, only the DBFILE is removed and 
    a message to the user in printed to screen.

    In a future version, this can optionally also 
    drop the test database.
    """
    with open(DBFILE, 'r') as f:
        name = f.read()
    os.remove(DBFILE)

    print('=' * 60)
    print("Finished.\nThe database '%s' is *not* dropped." % name)
    print('[DBNAME] %s' % name)