from datetime import datetime as dt

import pandas as pd
from sqlalchemy.orm import object_session

from metacatalog.models.entry import Entry

def read_from_internal_table(entry, datasource, start=None, end=None, **kwargs):
    # check data validity
    assert Entry.is_valid(entry)

    # get session
    session = object_session(entry)

    # get the tablename
    tablename = datasource.path

    # check if start and end date are set
    sql = "SELECT * FROM %s WHERE entry_id=%d" % (tablename, entry.id)
    if start is not None:
        sql += " AND tstamp >= '%s'" % (dt.strftime(start, '%Y-%m-%d %H:%M:%S'))
    if end is not None:
        sql += " AND tstamp <= '%s'" % (dt.strftime(end, '%Y-%m-%d %H:%M:%S'))

    # infer table column names order
    col_sql = 'select * from %s limit 0' % tablename
    col_names = list(pd.read_sql_query(col_sql, session.bind).columns.values)
    col_names.remove('entry_id')
    if 'index' in col_names:
        index_col = ['index']
        col_names.remove('index')
    elif 'tstamp' in col_names:
        index_col = ['tstamp']
        col_names.remove('tstamp')

    # load data
    df = pd.read_sql(sql, session.bind, index_col=index_col, columns=col_names)

    # map column names
    df.columns = [entry.variable.name if _col== 'value' else _col for _col in df.columns]

    return df


def read_from_local_csv_file(entry, datasource, **kwargs):
    # check validity
    assert Entry.is_valid(entry)

    # get the filename
    fname = datasource.path

    # read the file
    data = pd.read_csv(fname, index=None)

    # create index if needed
    if 'tstamp' in data.columns:
        data.set_index('tstamp', inplace=True)
    elif 'index' in data:
        data.set_index('index', inplace=True)
    
    # map column names
    df.columns = [entry.variable.name if _col== 'value' else _col for _col in df.columns]

    return data