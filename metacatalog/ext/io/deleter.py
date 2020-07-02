import os

from sqlalchemy.orm import object_session
import pandas as pd

from metacatalog.models import Entry, DataSource


def delete_from_internal_table(entry: Entry, datasource: DataSource, delete_source=True, **kwargs):
    """
    Deletes all data from the stored data-table. 

    .. warning::
        In case not all data is deleted, you **have to** 
        set :attr:`delete_source` to `False`, otherwise the 
        :class:`DataSource <metacatalog.models.DataSource>` is deleted and the data 
        itself it not reachable anymore.

    Parameters
    ----------
    entry : Entry
        The :class:`Entry <metacatalog.models.Entry>`, which requested the deletion.
    datasource : DataSource
        The :class:`DataSource <metacatalog.models.DataSource>`, which should be
        deleted.
    delete_source : bool
        If True (default) the :class:`DataSource <metacatalog.models.DataSource>` 
        will be deleted from the database.
    kwargs : keyword arguments
        where : str
            raw SQL filter term to filter the results before deleting

    """
    assert Entry.is_valid(entry)

    # get the tablename 
    tablename = datasource.path

    # build the filter
    filter_term = 'WHERE entry_id=%d' % entry.id
    if 'where' in kwargs:
        filter_term = '%s AND %s' % (filter_term, kwargs.get('where'))
    
    # build the query
    sql = 'DELETE FROM %s %s' % (tablename, filter_term)

    if kwargs.get('verbose', False):
        print("[SQL]: %s" % sql)
    
    # get a session
    session = object_session(entry)
    try:
        session.execute(sql)
        if delete_source:
            session.delete(datasource)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def delete_from_local_csv(entry: Entry, datasource: DataSource, delete_source=True, **kwargs):
    """
    Deletes all data from the associated CSV file. 

    .. warning::
        In case not all data is deleted, you **have to** 
        set :attr:`delete_source` to `False`, otherwise the 
        :class:`DataSource <metacatalog.models.DataSource>` is deleted and the data 
        itself it not reachable anymore.

    Parameters
    ----------
    entry : Entry
        The :class:`Entry <metacatalog.models.Entry>`, which requested the deletion.
    datasource : DataSource
        The :class:`DataSource <metacatalog.models.DataSource>`, which should be
        deleted.
    delete_source : bool
        If True (default) the :class:`DataSource <metacatalog.models.DataSource>` 
        will be deleted from the database.
    kwargs : keyword arguments
        Will be passed to the CSV read function.

    """
    assert Entry.is_valid(entry)

    # get the filepath
    filepath = datasource.path

    # open the file 
    df = pd.read_csv(filepath, index=None)
    df.where(df.entry_id!=entry.id).dropna(axis='index', inplace=True)
    
    # check if other data is left in the dataframe
    if df.empty:
        os.remove(filepath)
    else:
        df.to_csv(filepath, index=None)

    # check if the datasource should be deleted
    if delete_source:
        try:
            session = object_session(entry)
            session.delete(datasource)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
