"""
.. note::

    The ``append`` behavior is still a little naive. Basically, 
    data is appended to a file or table, but without checking 
    for duplicates. You have to check the integrity.

"""
from metacatalog.models import Entry, DataSource

from .importer import import_to_internal_table, import_to_local_csv_file


def append_to_internal_table(entry: Entry, datasource: DataSource, data, **kwargs):
    # force append
    kwargs['if_exists'] = 'append'
    return import_to_internal_table(entry=entry, datasource=datasource, data=data, **kwargs)


def append_to_local_csv_file(entry: Entry, datasource: DataSource, data, **kwargs):
    # force append
    kwargs['if_exists'] = 'append'
    return import_to_local_csv_file(entry=entry, datasource=DataSource, data=data, **kwargs)
