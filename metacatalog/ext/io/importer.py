import os

import pandas as pd
from sqlalchemy.orm import object_session

from metacatalog.models.entry import Entry
from metacatalog.models.timeseries import TimeseriesPoint


def import_to_internal_table(entry, datasource, data, mapping=None, **kwargs):
    """Import to internal DB

    The given data is imported into the table 
    as specified in the datasource. The data column names need to
    fit the names as implemented in the database. The mapping 
    keyword can be used to rename
    """
    # check that entry is valid
    assert Entry.is_valid(entry)
    
    if isinstance(data, pd.Series):
        data = pd.DataFrame(data)

    # reset the index
    imp = data.reset_index(level=0, inplace=False)
    
    # rename if mapping is given
    if mapping is not None and isinstance(mapping, dict):
        imp.rename(columns=mapping, inplace=True)

    # set entry_id
    if 'entry_id' not in imp.columns:
        imp['entry_id'] = entry.id

    # check if a session was passed
    if 'session' in kwargs.keys():
        session = kwargs['session']
    else:
        session = object_session(entry)

    if datasource.path is None:
        tablename = 'data_entry_%d' % entry.id
        datasource.path = tablename
        __update_datasource(datasource)
    else:
        tablename = datasource.path

    # get the available column names from the database
    sql = 'select * from %s limit 0' % tablename
    col_names = pd.read_sql_query(sql, session.bind).columns.values

    if not all([col in col_names for col in imp.columns.values]):
        raise ValueError('The input data has columns, that are not present in the database.\n %s' % ', '.join(col_names))
    
    # else import 
    if_exists = kwargs.get('if_exists', 'append')
    imp.to_sql(tablename, session.bind, index=None, if_exists=if_exists)


def import_to_local_csv_file(entry, datasource, data, **kwargs):
    """Import to CSV

    Saves timeseries data to a local CSV file.
    Any existing file will be overwritten. 
    The default location can be overwritten using the path keyword. 

    """
    assert Entry.is_valid(entry)

    # get the path
    if datasource.path is None:
        path = os.path.join(os.path.expanduser('~'))
    
    # check for filename
    if not path.endswith('.csv'):
        path = os.path.join(path, 'entry_%d.csv' % entry.id)
        datasource.path = path
        
        # save new path
        __update_datasource(datasource)

    # reset the index
    imp = data.reset_index(level=0, inplace=False)

    if_exists = kwargs.get('if_exists', 'replace')

    # save the data
    if if_exists == 'replace':
        imp.to_csv(path, index=None)
        
    elif if_exists == 'append':
        df = pd.read_csv(path, index=None)
        new_df = df.append(imp, ignore_index=True)
        new_df.to_csv(path, index=False)

    elif if_exists == 'fail':
        if os.path.exists(path):
            raise ValueError('%s already exists.' % path)
        else:
            data.to_csv(path, index=None)
    
    else:
        raise ValueError("if_exists has to be one of ['fail', 'append', 'replace']")


def __update_datasource(datasource):
    try:
        session = object_session(datasource)
        session.add(datasource)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

    return datasource