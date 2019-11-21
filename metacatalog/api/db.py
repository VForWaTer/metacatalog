import os

import pandas as pd

#from metacatalog import Base
from metacatalog.db.base import Base
from metacatalog.db.session import get_session
from metacatalog import DATAPATH
from metacatalog.models import DataSourceType, Unit, Variable, License, Keyword, PersonRole, EntryGroupType

IMPORTABLE_TABLES = dict(
    keywords=Keyword,
    datasource_types=DataSourceType,
    units=Unit,
    variables=Variable,
    licenses=License,
    person_roles=PersonRole,
    entrygroup_types=EntryGroupType
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


def _remove_nan_from_dict(d):
    out_d = dict()
    for k,v in d.items():
        if v is None:
            continue
        elif isinstance(v, dict):
            out_d[k] = _remove_nan_from_dict(v)
        else:
            out_d[k] = v
    return out_d


def import_table_data(fname, InstanceClass):
    df = pd.read_csv(os.path.join(DATAPATH, fname))

    # replace nan with None
    df = df.where(df.notnull(), None)

    # build an instance for each line and return
    return [InstanceClass(**_remove_nan_from_dict(d)) for d in df.to_dict(orient='record')]


def import_direct(session, table_name, file_name):
    # load the data
    df = pd.read_csv(file_name)

    # replace nan with None
    df = df.where(df.notnull(), None)

    # directly inject into db
    df.to_sql(table_name, session.bind, index=False, if_exists='append')


def update_sequence(session, table_name, sequence_name=None):
    """
    On insert with given id, PostgreSQL does not update the sequence 
    for autoincrement. Thus tables with defaults cannot use autoincremented
    PK anymore. Thus, the sequence is set to the current value
    """
    if sequence_name is None:
        sequence_name = '%s_id_seq' % table_name
    sql = "SELECT setval('%s', (SELECT MAX(id) from %s), true);" % (sequence_name, table_name)

    res = session.execute(sql)
    return res.scalar()
    


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

        # keywords has to be handled extra as there is a self-reference
        if table == 'keywords':
            print('Populating %s' % table)
            import_direct(session, table, os.path.join(DATAPATH, '%s.csv' % table))
            print('Finished %s' % table)
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
    
    for table in IMPORTABLE_TABLES.keys():
        if table in ignore_tables:
            continue
        last_id = update_sequence(session, table)
        print('Set %s_id_seq to %d' % (table, last_id))
    print('Done.')
 