from datetime import datetime as dt

import pandas as pd
from sqlalchemy.orm import object_session

from metacatalog.models.entry import Entry

def read_from_internal_table(entry, datasource, **kwargs):
    # check data validity
    assert Entry.is_valid(entry)

    # get session
    session = object_session(entry)

    # get the tablename
    tablename = datasource.path

    # check if start and end date are set
    if 'start' in kwargs.keys() or 'end' in kwargs.keys():
        sql = "SELECT * FROM %s WHERE entry_id=%d" % (tablename, entry.id)
        if 'start' in kwargs.keys():
            sql += " AND tstamp >= '%s'" % (dt.strftime(kwargs['start'], '%Y-%m-%d %H:%M:%S'))
        if 'end' in kwargs.keys():
            sql += " AND tstamp <= '%s'" % (dt.strftime(kwargs['end'], '%Y-%m-%d %H:%M:%S'))
    else:
        sql = tablename

    # infer table column names order
    sql = 'select * from %s limit 0' % tablename
    col_names = list(pd.read_sql_query(sql, session.bind).columns.values)
    col_names.remove('entry_id')
    if 'index' in col_names:
        index_col = ['index']
        col_names.remove('index')
    elif 'tstamp' in col_names:
        index_col = ['tstamp']
        col_names.remove('tstamp')

    # load data
    df = pd.read_sql(sql, session.bind, index_col=index_col, columns=col_names)

    if len(df.columns) == 1:
        df.columns = [entry.variable.name]

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
    
    if len(data.columns) == 1:
        data.columns = [entry.variable.name]

    return data