import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
        return create_engine(URI)
    elif mode.lower() == 'session':
        Session = sessionmaker(bind=create_engine(URI))
        return Session()