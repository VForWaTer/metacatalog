from typing import List, TYPE_CHECKING
if TYPE_CHECKING:
    from sqlalchemy.orm import Session
import os

import pandas as pd
from pandas.errors import ParserError
import numpy as np

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

def connect_database(*args, **kwargs) -> 'Session':
    """
    Connect to database and returns a :class:`Session <sqlalchemy.orm.Session>`
    to the database. 
    
    You can either pass args and kwargs as accepted by sqlachemy's 
    :func:`create_engine <sqlalchemy.create_engine>` method or pass a name of a stored connection string. 
    Connection strings can be stored using :func:`save_connection <metacatalog.db.save_connection>` method.
    Empty arguments will load the default connection string, if there is any.

    Parameters
    ----------
    *args
        See sqlalchemy :func:`create_engine <sqlalchemy.create_engine>` for a full list.
        The only additional  argument is a stored connection string, if any. The function 
        will first check for a stored connection of given name and only if none is found, pass 
        ``*args`` down to :func:`create_engine <sqlalchemy.create_engine>`. 
        If ``len(args)==0``, a stored connection of name ``'default'`` will be loaded.
    **kwargs
        Only used if no stored connection is loaded. 
    
    See Also
    --------
    :func:`save_connection <metacatalog.db.save_connection>`
    
    """
    # get session
    session = get_session(*args, **kwargs)

    return session


def _set_migration_head(session: 'Session') -> None:
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
def _log(session: 'Session', message: str, code=models.LogCodes.info) -> None:
    try:
        log = models.Log(code=code, description=message, migration_head=migration.get_remote_head_id(session))
        session.add(log)
        session.commit()
    except:
        session.rollback()


def create_tables(session: 'Session') -> None:
    """
    Create all tables in the database using the given 
    `Session <sqlalchemy.Session>` instance.

    Parameters
    ----------
    session : sqlalchemy.Session
        Session instance connected to the database.
    
    """
    Base.metadata.create_all(session.bind)
    _log(session, 'Creating tables')

    # set the latest version
    _set_migration_head(session)


def _remove_nan_from_dict(d):
    out_d = dict()
    for k,v in d.items():
        if pd.isnull(v):
            continue
        elif isinstance(v, dict):
            out_d[k] = _remove_nan_from_dict(v)
        else:
            out_d[k] = v
    return out_d


def import_table_data(fname: str, InstanceClass: Base, array_col_name: str = None) -> List[Base]:
    try:
        df = pd.read_csv(os.path.join(DATAPATH, fname))
    except ParserError as e:
        print('[ERROR] default table data file corrupted.\nFile: %s\nOriginal Exception: %s' % (fname, str(e)))
        return []

    # replace nan with None
    df = df.replace({np.nan: None})

    # handle arrays
    if array_col_name is not None:
        df[array_col_name] = [[cell] for cell in df[array_col_name].values]

    # build an instance for each line and return
    return [InstanceClass(**_remove_nan_from_dict(d)) for d in df.to_dict('records')]


def import_direct(session: 'Session', table_name: str, file_name: str) -> None:
    # load the data
    df = pd.read_csv(file_name)

    # replace nan with None
    df = df.where(df.notnull(), None)

    # directly inject into db
    df.to_sql(table_name, session.bind, index=False, if_exists='append')


def update_sequence(session: 'Session', table_name: str, sequence_name: str = None, to_value: int = None) -> None:
    """
    On insert with given id, PostgreSQL does not update the sequence 
    for autoincrement. Thus tables with defaults cannot use autoincremented
    PK anymore. Thus, the sequence is set to the current value.

    Parameter
    ---------
    session : sqlalchemy.Session
        Session to the database.
    table_name : str
        The name of the table, the sequence is bound to.
    sequence_name : str
        Optional. The default sequence name is {table_nane}_id_seq. If
        another sequence_name is given, that will be used.
    to_value : int
        The new value of the sequence

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
    

def populate_defaults(session: 'Session', ignore_tables: List[str] = [], bump_sequences: int = 10000) -> None:
    """
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

        # keywords and datatypes have to be handled extra as there is a self-reference
        if table in ['keywords', 'datatypes']:
            print('Populating %s' % table)
            import_direct(session, table, os.path.join(DATAPATH, '%s.csv' % table))
            print('Finished %s' % table)
            continue
        
        elif table == 'variables':
            instances = import_table_data('variables.csv', InstanceClass, array_col_name='column_names')
        else:
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
 