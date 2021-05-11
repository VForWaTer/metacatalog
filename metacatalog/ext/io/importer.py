import os

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import object_session
from sqlalchemy.dialects.postgresql import ARRAY

from metacatalog.models.entry import Entry
from metacatalog.models.timeseries import TimeseriesPoint


def import_to_internal_table(entry, datasource, data, force_data_names=False, **kwargs):
    """Import to internal DB

    The given data is imported into the table
    as specified in the datasource.
    If force_data_names=True the column names of the imported data are saved in
    the datasource, otherwise the standard column names in
    entry.variable.column_names are used. The column names in
    datasource.data_names are used when exporting the data.
    """
    # check that entry is valid
    assert Entry.is_valid(entry)

    if isinstance(data, pd.Series):
        data = pd.DataFrame(data)

    # reset the index
    imp = data.reset_index(level=0, inplace=False)

    # check if a session was passed
    if 'session' in kwargs.keys():
        session = kwargs['session']
    else:
        session = object_session(entry)

    # get index, drop it afterwards
    if 'index' in imp.columns:
        index = imp.index
        imp.drop('index', axis=1, inplace=True)
    elif 'tstamp' in imp.columns:
        index = imp.tstamp
        imp.drop('tstamp', axis=1, inplace=True)

    # get the column names - exclude everthing that stores precisions
    data_columns = [col for col in imp.columns.tolist() if not col.startswith('precision')]

    # get the precision columns
    precision_columns = [col for col in imp.columns.tolist() if col.startswith('precision')]

    # set entry_id
    if 'entry_id' not in imp.columns:
        imp['entry_id'] = entry.id

    # save column names in datasource.data_names (excluding precision)
    if len(data_columns) != len(entry.variable.column_names):
        force_data_names = True
    if force_data_names:
        datasource.data_names = data_columns
    else:
        datasource.data_names = entry.variable.column_names

    # get the path / table name into which the data should be imported
    if datasource.path is None:
        tablename = 'data_entry_%d' % entry.id
        datasource.path = tablename
        __update_datasource(datasource)
    else:
        tablename = datasource.path

    # transform the data into a list of arrays
    values = [row for row in imp[data_columns].values]
    precision = [row for row in imp[precision_columns].values]

    # make importer.py compatible with the (old) 1D timeseries table
    if tablename == 'timeseries':
        # explicitly map the column types
        dtypes = {
            'tstamp': sa.TIMESTAMP,
            'value': sa.NUMERIC,
            'precision': sa.NUMERIC
        }

        # convert 1D np.ndarray data and precision to type float
        values = [number for array in values for number in array]
        precision = [number for array in precision for number in array]

        # the list comprehension above creates an empty list if precision is empty:
        if not precision:
            imp_data = pd.DataFrame(data={'tstamp': index, 'value': values})
            imp_data['entry_id'] = entry.id
        else:
            imp_data = pd.DataFrame(data={'tstamp': index, 'value': values, 'precision': precision})
            imp_data['entry_id'] = entry.id

        # else import
        if_exists = kwargs.get('if_exists', 'append')
        imp_data.to_sql(tablename, session.bind, index=None, dtype=dtypes, if_exists=if_exists)
    else:
        # else: the (new) timeseries_array is used
        # explicitly map the column types
        dtypes = {
            'tstamp': sa.TIMESTAMP,
            'data': ARRAY(sa.REAL),
            'precision': ARRAY(sa.REAL)
        }

        imp_data = pd.DataFrame(data={'tstamp': index, 'data': values, 'precision': precision})
        imp_data['entry_id'] = entry.id

        # else import
        if_exists = kwargs.get('if_exists', 'append')
        imp_data.to_sql(tablename, session.bind, index=None, dtype=dtypes, if_exists=if_exists)


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
