import os

import pandas as pd

from metacatalog import Base
from metacatalog.db import get_session
from metacatalog import DATAPATH
from metacatalog.models import DataSourceType, Unit, Variable, License

IMPORTABLE_TABLES = dict(
    datasource_types=DataSourceType,
    units=Unit,
    variables=Variable,
    licenses=License
)

def connect_database(*args, **kwargs):
    """Connect to database

    Returns a `Session <sqlalchemy.Session>` to the database.
    Either pass args and kwargs as accepted by sqlachemy's 
    `create_engine <sqlalchemy.create_engine>` method or pass 
    a name of a stored connection string.
    Connection strings can be stored using 
    `save_connection <metacatalog.db.save_connection>` method.
    Empty arguments will load the default connection string, if 
    there is any.
    """
    # get session
    session = get_session(*args, **kwargs)

    return session


def create_tables(session):
    """Create tables

    Create all tables in the database using the given 
    `Session <sqlalchemy.Session>` instance.

    Params
    ------
    session : sqlalchemy.Session
        Session instance connected to the database.
    
    """
    Base.metadata.create_all(session.bind)


def import_table_data(fname, InstanceClass):
    df = pd.read_csv(os.path.join(DATAPATH, fname))

    # build an instance for each line and return
    return [InstanceClass(**d) for d in df.to_dict(orient='record')]


def populate_defaults(session, ignore_tables=[]):
    """Import default data

    Populates many lookup and auxiliary tables with useful default 
    information. The actual data is read from a data subdirectory 
    inside this module and can therefore easily be adapted. 
    Any table name supplied in ignore_tables will be omitted.

    As of now, the following tables can be pre-polulated:

    * datasource_types
    * units
    * variables

    Params
    ------
    session : sqlalchemy.Session
        Session instance connected to the database.
    ignore_tables : list
        List of tables to be omitted. Be aware that the actual 
        table name in the database has to be supplied, not the 
        name of the model in Python.

    """
    for table, InstanceClass in IMPORTABLE_TABLES.items():
        if table in ignore_tables:
            continue
        
        # get the classes
        instances = import_table_data('%s.csv' % table, InstanceClass)

        # add
        try:
            print('Populating %s' % table)
            session.add_all(instances)
            session.commit()
        except Exception as e:
            print('Failed.\n%s' % str(e))
            session.rollback()
        print('Finished %s' % table)
    print('Done.')
 