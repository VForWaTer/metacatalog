"""
The extensions module can be used to register global classes to metacatalog 
on startup. As of now, this is only used for Custom reader and importer 
functions.

If you register new function, be sure to provide a sufficient interface.

Reader
------
A reader function has to look like:

.. code-block:: python

    from metacatalog.models import Entry, DataSource

    def reader_name(entry: Entry, datasource: DataSource, **kwargs):
        [...]

The kwargs can be given by the user on requesting data, or they were 
stored by the database admin as JSON in the `DataSource.args` attribute

Importer
--------
An importer function has to look like:

.. code-block:: python

    from metacatalog.models import Entry, DataSource

    def importer_name(entry: Entry, data, datasource: Datasource, **kwargs):
        [...]

The kwargs can be given by the user on importing data, or they were 
stored by the database admin as JSON in the `DataSource.args` attribute

The user has to make sure that the data given as data attribute can 
actually be stored at the path given in the datasource. This should be 
handled in the importer function 
(e.g. check table exists or file location is writeable etc.)

"""
from .importer import import_to_internal_table, import_to_local_csv_file
from .reader import read_from_internal_table, read_from_local_csv_file
from .exceptions import NoReaderFoundWarning, NoImporterFoundWarning

READER = dict(
    internal=read_from_internal_table,
    csv=read_from_local_csv_file
)
IMPORTER = dict(
    internal=import_to_internal_table,
    csv=import_to_local_csv_file
)


def get_reader(name: str) -> callable:
    """
    Return the reader function of name. 
    This function is used by 
    :class:`DataSource <metacatalog.models.DataSource>`, do not 
    use it directly.

    Parameters
    ----------
    name : name
        The name of datasource types that should use this function to 
        read data as specified in the datasource.

        .. note::
            name is not case sensitive, please use only lowercase names
    
    Returns
    -------
    reader : callable
        The reader function for the requested datasource type
    """
    if name.lower() not in READER.keys():
        raise NoReaderFoundWarning("A reader of name '%s' is not registered." % name)
    return READER[name.lower()]


def get_importer(name: str) -> callable:
    """
    Return the importer function of name.
    This function is used by
    :class:`DataSource <metacatalog.models.DataSource>`, do not
    use it directly.

    Parameters
    ----------
    name : name
        The name of datasource types that should use this function to
        read data as specified in the datasource.

        .. note::
            name is not case sensitive, please use only lowercase names

    Returns
    -------
    importer : callable
        The importer function for the requested datasource type
    """
    if name.lower() not in IMPORTER.keys():
        raise NoImporterFoundWarning("An importer of name '%s' is not registered." % name)
    return IMPORTER[name.lower()]


def register(func: callable, name: str, type: str):
    if type.lower() == 'reader':
        if name.lower() in READER.keys():
            raise Warning("A reader of name '%s' already exists." % name)
        READER[name.lower()] = func
    elif type.lower() == 'importer':
        if name.lower() in IMPORTER.keys():
            raise Warning("An importer of name '%s' already exists." % name)
        IMPORTER[name.lower] = func
    else:
        raise ValueError("'%s' is not a valid type." % type)


def register_importer(func: callable, name: str):
    """
    Register new importer function for metacatalog. The name
    has to match the DataSourceType of the datasource 
    using this importer. You may have to add a new record to 
    DataSourceType to make the function work.

    An importer function has to look like:

    .. code-block:: python

        from metacatalog.models import Entry, DataSource

        def importer_name(entry: Entry, data, datasource: Datasource, **kwargs):
            [...]

    The kwargs can be given by the user on importing data, or they were 
    stored by the database admin as JSON in the `DataSource.args` attribute

    The user has to make sure that the data given as data attribute can 
    actually be stored at the path given in the datasource. This should be 
    handled in the importer function 
    (e.g. check table exists or file location is writeable etc.)

    Parameters
    ----------
    func : callable
        The importer function
    name : name
        The name of datasource types that should use this function to 
        import data as specified in the datasource.

        .. note::
            name is not case sensitive, please use only lowercase names

    """
    register(func=func, name=name, type='importer')


def register_reader(func: callable, name: str):
    """
    Register new reader function for metacatalog. The name
    has to match the DataSourceType of the datasource 
    using this reader. You may have to add a new record to 
    DataSourceType to make the function work.

    A reader function has to look like:

    .. code-block:: python

        from metacatalog.models import Entry, DataSource

        def reader_name(entry: Entry, datasource: DataSource, **kwargs):
            [...]

    The kwargs can be given by the user on requesting data, or they were 
    stored by the database admin as JSON in the `DataSource.args` attribute

    Parameters
    ----------
    func : callable
        The importer function
    name : name
        The name of datasource types that should use this function to 
        import data as specified in the datasource.
        .. note::
            name is not case sensitive, please use only lowercase names    
    
    """
    register(func=func, name=name, type='reader')
