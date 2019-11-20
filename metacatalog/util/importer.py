import os

import pandas as pd
from sqlalchemy.orm import object_session

from metacatalog.models.entry import Entry
from metacatalog.models.timeseries import TimeseriesPoint


def import_to_interal_table(entry, timeseries, datasource, value_col='value', date_col='tstamp', **kwargs):
    """Import to DB

    Saves the given timeseries into a table. 
    The data should have a datetime index called tstamp and 
    the values column called 'values'.

    """
    #check_valid_entry(entry)
    #check_valid_timeseries(timeseries)
    assert Entry.is_valid(entry)
    assert TimeseriesPoint.is_valid_timeseries(timeseries)

    if isinstance(timeseries, pd.Series):
        timeseries = pd.DataFrame(timeseries)

    # reset the datetime index
    data = timeseries.reset_index(level=0, inplace=False)

    # add the entry_id column
    data['entry_id'] = entry.id

    # rename columns:
    data.rename(columns={value_col: 'value', date_col: 'tstamp'}, inplace=True)

    # check
    if not 'value' in data.columns or not 'tstamp' in data.columns:
        raise AttributeError('Could not parse correct columns. Use the value_col and date_col arguments')

    # now all columns are present, set the index
    data.set_index(['entry_id', 'tstamp'], inplace=True)

    # extract the needed columns
    data = data[['value']].copy()

    # upload into the new table
    if 'session' in kwargs.keys():
        session = kwargs['session']
    else:
        session = object_session(entry)

    # if tablename not set, ifer
    if datasource.path is None:
        tablename = 'timeseries_data_%d' % entry.id
    else:
        tablename = datasource.path

    # upload
    data.to_sql(tablename, session.bind, if_exists='append')


def import_to_local_csv_file(entry, timeseries, datasource, **kwargs):
    """Import to CSV

    Saves timeseries data to a local CSV file.
    Any existing file will be overwritten. 
    The default location can be overwritten using the path keyword. 

    """
    assert Entry.is_valid(entry)
    assert TimeseriesPoint.is_valid_timeseries(timeseries)

    # get the path
    if datasource.path is None:
        path = os.path.join(os.path.expanduser('~'))
    
    # check for filename
    if not path.endswith('.csv'):
        path = os.path.join(path, 'entry_%d.csv' % entry.id)
        datasource.path = path
        
        # save new path
        try:
            session = object_session(datasource)
            session.add(datasource)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

    if_exists = kwargs.get('if_exists', 'overwrite')
    if not if_exists == 'overwrite':
        raise NotImplementedError("Only 'overwrite' supported for now.")
    
    # save the data
    timeseries.to_csv(path)