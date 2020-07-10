import os
from os.path import join as pjoin

import pandas as pd
from pandas.errors import ParserError

#from metacatalog import Base
from metacatalog import BASEPATH, __version__
from metacatalog.db.base import Base
from metacatalog.db.session import get_session
from metacatalog.db import migration
from metacatalog import DATAPATH
from metacatalog import models

IMPORTABLE_TABLES = dict(
    thesaurus=models.Thesaurus,
    keywords=models.Keyword,
    datasource_types=models.DataSourceType,
    datatypes=models.DataType,
    units=models.Unit,
    variables=models.Variable,
    licenses=models.License,
    person_roles=models.PersonRole,
    entrygroup_types=models.EntryGroupType
)

def connect_database(*args, **kwargs):
    """Connect to database

    Returns a 
    `sqlalchemy Session <https://docs.sqlalchemy.org/en/latest/orm/session_api.html#sqlalchemy.orm.session.Session>`_ 
    to the database. 
    
    You can either pass args and kwargs as accepted by sqlachemy's 
    `create_engine <sqlalchemy.create_engine>` method or pass a name of a stored connection string. 
    Connection strings can be stored using `save_connection <metacatalog.db.save_connection>` method.
    Empty arguments will load the default connection string, if there is any.

    Parameters
    ----------
    *args
        See sqlalchemy `create_engine <https://docs.sqlalchemy.org/en/latest/core/engines.html>`_ for a full list.
        The only additional  argument is a stored connection string, if any. The function 
        will first check for a stored connection of given name and only if none is found, pass 
        *args down to `create_engine`. If `len(args)==0`, a stored connection of name 
        `'default'` will be loaded.
    **kwargs
        Only used if no stored connection is loaded. 
    
    See Also
    --------
    `save_connection <metacatalog.db.save_connection>`
    
    """
    # get session
    session = get_session(*args, **kwargs)

    return session


def _set_migration_head(session):
    """
    Set the migration log to the current release.
    This has to be set on new database instances to make the logs 
    point to the correct migration version. Otherwise 
    the migration would run conflicting code. 

    """
    local_head = migration.get_local_head_id()
    try:
        remote_head = migration.get_remote_head_id(session)
    except:
        remote_head = 0
    if local_head > remote_head:
        log = models.Log(
            code=models.LogCodes.migration, 
            description='Bump HEAD after clean install using metacatalog==%s' %  __version__, 
            migration_head=local_head)
        session.add(log)
        session.commit()

# TODO turn this into an actual logging module
def _log(session, message, code=models.LogCodes.info):
    try:
        log = models.Log(code=code, description=message, migration_head=migration.get_remote_head_id(session))
        session.add(log)
        session.commit()
    except:
        session.rollbac()



def create_tables(session):
    """Create tables

    Create all tables in the database using the given 
    `Session <sqlalchemy.Session>` instance.

    Parameters
    ----------
    session : sqlalchemy.Session
        Session instance connected to the database.
    
    """
    Base.metadata.create_all(session.bind)
    _log(session, 'Creting tables')

    # set the latest version
    _set_migration_head(session)


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
    try:
        df = pd.read_csv(os.path.join(DATAPATH, fname))
    except ParserError as e:
        print('[ERROR] default table data file corrupted.\nFile: %s\nOriginal Exception: %s' % (fname, str(e)))
        return []

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


def update_sequence(session, table_name, sequence_name=None, to_value=None):
    """
    On insert with given id, PostgreSQL does not update the sequence 
    for autoincrement. Thus tables with defaults cannot use autoincremented
    PK anymore. Thus, the sequence is set to the current value
    """
    if sequence_name is None:
        sequence_name = '%s_id_seq' % table_name
    if to_value is None:
        val = '(SELECT MAX(id) from %s)' % table_name
    else:
        val = '%d' % to_value
    sql = "SELECT setval('{seq}', {val}, true);".format(seq=sequence_name, val=val)

    res = session.execute(sql)
    return res.scalar()
    


def populate_defaults(session, ignore_tables=[], bump_sequences=10000):
    """Import default data

    Populates many lookup and auxiliary tables with useful default 
    information. The actual data is read from a data subdirectory 
    inside this module and can therefore easily be adapted. 
    Any table name supplied in ignore_tables will be omitted.

    .. warning::
        Do only ignore tables if you know what you are doing.
        There are dependencies (like variables, units and keywords) and 
        some of the API is depending on some defaults (like datatypes).

    As of now, the following tables can be pre-polulated:

    * datasource_types
    * datatypes
    * entrygroup_types
    * keywords
    * licenses
    * person_roles
    * thesaurus
    * units
    * variables

    Parameters
    ----------

    session : sqlalchemy.Session
        Session instance connected to the database.
    ignore_tables : list
        List of tables to be omitted. Be aware that the actual 
        table name in the database has to be supplied, not the 
        name of the model in Python.
    bump_sequences : int, None
        If integer (default), the primary key sequences will be 
        set to this value. It is recommended to use a high value like 
        the default 10,000 to allow for future including of new 
        pre-populated values. Otherwise, these might have integrity 
        conflicts with your database objects.

    """
    # make sure that thesaurus is not ignored, when keywords are imported
    if 'thesaurus' in ignore_tables and 'keywords' not in ignore_tables:
        raise AttributeError('You must not ignore thesaurus if keywords is not ignored')
    
    # same applies to variables and units
    if 'units' in ignore_tables and 'variables' not in ignore_tables:
        raise AttributeError('You must not ignore units if variables not ignored')

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
            if len(instances) > 0:
                print('Populating %s' % table)
                session.add_all(instances)
                session.commit()
            else:
                print('Can\'t populate %s.' % table)
        except Exception as e:
            print('Failed.\n%s' % str(e))
            session.rollback()
        print('Finished %s' % table)
    
    # log
    _log(session, "Populated default values. Ignored: ['%s]" % ','.join(ignore_tables))

    for table in IMPORTABLE_TABLES.keys():
        if table in ignore_tables:
            continue
        if isinstance(bump_sequences, int) and bump_sequences > 0:
            last_id = update_sequence(session, table, to_value=bump_sequences)
        else: 
            last_id = update_sequence(session, table)
        if last_id is not None:
            print('Set %s_id_seq to %d' % (table, last_id))
        else:
            print('Setting %s_id_seq failed as it seems to have no entries.' % table)
    print('Done.')
 