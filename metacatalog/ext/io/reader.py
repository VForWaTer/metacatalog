from datetime import datetime as dt

import pandas as pd
import numpy as np
from sqlalchemy.orm import object_session
from sqlalchemy.dialects.postgresql import ARRAY

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
    col_names_sql = list(pd.read_sql_query(col_sql, session.bind).columns.values)
    col_names_sql.remove('entry_id')

    if 'index' in col_names_sql:
        index_col_sql = ['index']
        col_names_sql.remove('index')
    elif 'tstamp' in col_names_sql:
        index_col_sql = ['tstamp']
        col_names_sql.remove('tstamp')

    # load data
    df_sql = pd.read_sql(sql, session.bind, index_col=index_col_sql, columns=col_names_sql)

    # always use data_names from datasource as column names when exporting the data
    col_names = datasource.data_names

    # if the column 'data' exists, ND array data must be unstacked
    if 'data' in df_sql.columns:
        # unstack multi-dimensional data into the single columns
        rawvalues = np.vstack(df_sql['data'].values)

        # unstack precision (precision1, precision2, ...)
        rawprecision = np.vstack(df_sql['precision'].values)

        if not all(x is None for x in np.hstack(rawprecision)): # check if precision contains any values
            # add precision column names to col_names, if data is contained
            for i in range(1, len(rawprecision[0])+1):
                precision_col = 'precision%s' % i
                col_names.append(precision_col)
        else:
            rawprecision = np.array([], dtype=np.int64).reshape(len(rawprecision),0)

        # horizontally stack data and precission
        raw = np.hstack([rawvalues, rawprecision])

        df = pd.DataFrame(data=raw, columns=col_names, dtype=np.float64, index=df_sql.index)
    elif 'value' in df_sql.columns:
        # if 'value' appears in the column names, the old routine for 1D data is used
        df = df_sql.copy()
        df.drop(['entry_id'], axis=1, inplace=True)

        # map column names
        df.columns = [datasource.data_names[0] if _col== 'value' else _col for _col in df.columns]

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
