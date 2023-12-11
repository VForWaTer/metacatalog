from metacatalog.models import Entry

from sqlalchemy.orm import object_session
import os
import pandas as pd
import datetime as dt


def set_data(entry: Entry, data_path: str, **kwargs):
    """
    TODO: should we accept a pandas.DataFrame here? I would say yes, as we also need to read csv files into a DataFrame to save it to the database.
    
    """
    assert Entry.is_valid(entry)

    # get the datasource path of the entry
    datasource_path = entry.datasource.path

    # TODO: 
    # new schema (one table for each entry) -> alter datasource_path automatically with the tablename?
    # -> entry_id column is not needed anymore, backwards kompatibel: column names checken, wenn entry_id vorhanden, dann
    # nach entry_id filtern, ansonsten ganz Tabelle nehmen3

    # Spalte entry_id behalten wir

    # use absolute paths
    data_path = os.path.abspath(data_path)

    # load the data
    data = pd.read_csv(data_path)

    # TODO: should we still handle precision column? -> keine precision column mehr, aber tstamp column
    # datetime index immer in tstamp umbenennen

    validate(data)

    # get the session from the Entry object
    session = object_session(entry)

    data.to_sql(datasource_path, session.bind, index=None)#, dtype=dtypes, if_exists=if_exists) #TODO: handle if_exists and dtypes(? -> new schema logic -> handling dtypes gets hard as there can be a lot of columns with different dtypes, BUT: handle tstamp column)

    return None


def get_data(entry: Entry, output_path: str, start=None, end=None, **kwargs):
    """
    Get the data of an Entry from the database and save it to a csv file
    in output_path.
    
    """
    # check data validity
    assert Entry.is_valid(entry)

    # get session
    session = object_session(entry)

    # get the tablename
    tablename = entry.datasource.path

    # build sql query
    sql = f"SELECT * FROM {tablename} WHERE entry_id=%d" % (tablename, entry.id) # this is valid for the old schema, but not for the new one (one table for each entry)

    # alter sql query to filter if start or end date are set
    if start or end:
        sql = filter_data(sql, start, end)

    # load data
    data = pd.read_sql(sql, session.bind)

    # save the data to the output_path
    data.to_csv(output_path, index=None)

    return None


def validate(data: pd.DataFrame) -> bool:
    """
    Validate the given data_path.

    Parameters
    ----------
    data_path : str
        Path to the data source.
    

    """
    # TODO: what do we want to validate here? -> tstamp column?, temporal_range? 
    pass


def filter_data(sql, start, end):
    """
    Extend the sql query to filter the data by start and end date.
    
    """
    if start is not None:
        sql += " AND tstamp >= '%s'" % (dt.strftime(start, '%Y-%m-%d %H:%M:%S'))
    if end is not None:
        sql += " AND tstamp <= '%s'" % (dt.strftime(end, '%Y-%m-%d %H:%M:%S'))

    return sql