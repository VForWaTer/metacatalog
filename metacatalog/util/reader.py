from datetime import datetime as dt

import pandas as pd
from sqlalchemy.orm import object_session

from metacatalog.models.entry import Entry

def read_from_interal_table(entry, datasource, **kwargs):
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
    
    # load data
    df = pd.read_sql(sql, session.bind, index_col=['tstamp'], columns=['value'])

    df.columns = [entry.variable.name]

    return df


def read_from_local_csv_file(entry, datasource, **kwargs):
    # check validity
    assert Entry.is_valid(entry)

    # get the filename
    fname = datasource.path

    # read the file
    timeseries = pd.read_csv(fname)

    # create index if needed
    if 'tstamp' in timeseries.columns:
        timeseries.set_index('tstamp', inplace=True)
    
    timeseries.columns = [entry.variable.name]

    return timeseries