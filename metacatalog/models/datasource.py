import json
from functools import wraps

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, object_session

from metacatalog.db.base import Base
from metacatalog.util.ext import get_reader, get_importer


class DataSourceType(Base):
    r"""Data Source Type

    Model to represent a type of datasource. 
    
    Note
    ----
    While it is possible to add more records to the table, 
    this is the only Class that needs actual Python functions to 
    handle the database input. Usually, each type of datasource 
    relies on a specific :mod:`importer <metacatalog.util.importer>` 
    and reader :mod:`reader <metacatalog.util.reader>` that can use 
    the information saved in a :class:`DataSource` to perform I/O operations.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    name : str
        A short (64) name for the Type. Should not contain any whitespaces
    title : str
        The full title of this Type.
    description : str
        Optional description about this type
    """
    __tablename__ = 'datasource_types'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    title = Column(String, nullable=False)
    description = Column(String)

    # relationships
    sources = relationship("DataSource", back_populates='type')

    def to_dict(self, deep=False) -> dict:
        """To dict

        Return the model as a python dictionary.

        Parameters
        ----------
        deep : bool
            If True, all related objects will be included as 
            dictionary as well and deep will be passed down.
            Defaults to False

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            id = self.id,
            name = self.name,
            title = self.title
        )

        # set optionals
        if self.description is not None:
            d['description'] = self.description
        
        # deep loading
        if deep:
            d['sources'] = [s.to_dict(deep=True) for s in self.sources]
        
        return d

    def __str__(self):
        return '%s data source <ID=%d>' % (self.name, self.id)


class DataSource(Base):
    r"""DataSource

    Model to represent a datasource of a specific 
    :class:`Entry <metacatalog.models.Entry>`. The datasource further specifies 
    an :class:`DataSourceType` by setting a ``path`` and ``args``.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    path : str
        Path to the actual data. Depending on type, this can be a filepath, SQL 
        tablename or URL.
    args : str
        Optional. If the I/O classes need further arguments, these can be stored 
        as a JSON-serializable str. Will be parsed into a dict and passed to the
        I/O functions as **kwargs.
    type_id : int
        Foreign key referencing the :class:`DataSourceType`. 
    type : metacatalog.models.DataSourceType
        The referenced :class:`DataSourceType`. Can be used instead of setting 
        ``type_id``.

    Example
    -------
    There is a :class:`DataSourceType` of ``name='internal'``, which handles 
    I/O operations on tables in the same database. The datasource itself 
    will then store the tablename as ``path``. It can be linked to 
    :class:`Entry <metacatalog.models.Entry>` in a 1:n relationship. 
    This way, the admin has the full control over data-tables, while still using 
    common I/O classes.

    """
    __tablename__ = 'datasources'

    # column
    id = Column(Integer, primary_key=True)
    type_id = Column(Integer, ForeignKey('datasource_types.id'), nullable=False)
    path = Column(String, nullable=False)
    args = Column(String)

    # relationships
    entries = relationship("Entry", back_populates='datasource')
    type = relationship("DataSourceType", back_populates='sources')

    @classmethod
    def is_valid(cls, ds):
        return hasattr(ds, 'path') and isinstance(ds, DataSource)

    def to_dict(self, deep=False) -> dict:
        """To dict

        Return the model as a python dictionary.

        Parameters
        ----------
        deep : bool
            If True, all related objects will be included as 
            dictionary. Defaults to False

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            id = self.id,
            path = self.path,
            type=self.type.to_dict(deep=False)
        )

        # set optionals
        if self.args is not None:
            d['args'] = self.parse_args()
            
        
        # deep loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        
        return d

    def parse_args(self):
        r"""Load args

        Note
        ----    
        Load the contents of the args column as assumed JSON string.
        This will be passed to the importer/adder function as **kwargs.
        Therefore this is only useful for a DB admin and should not be 
        exposed to the end-user.

        """
        # return empty dict
        if self.args is None:
            return dict()

        # parse and return
        else:
            return json.loads(self.args)
    
    def save_args_from_dict(self, args_dict, commit=False):
        """Save to args

        Save all given keyword arguments to the database. 
        These are passed to the importer/adder functions as **kwargs.

        Parameters
        ----------
        args_dict : dict
            Dictionary of JSON-serializable keyword arguments that 
            will be stored as a JSON string in the database. 

        Note
        ----
        All kwargs need to be json encodeable. This function is only useful
        for a DB admin and should not be exposed to the end-user

        See Also
        --------
        parse_args

        """
        self.args = json.dumps(args_dict)

        if commit:
            try:
                session = object_session(self)
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    def get_source_importer(self):
        """Get importer

        This function is usually called by a 
        :class:`Entry <metacatalog.Entry>` object. It returns a function 
        that will import the data into the correct source.

        """
        func = get_importer(self.type.name)

        def injected_func(entry, timeseries, **kwargs):
            return func(entry, timeseries, self, **kwargs)
        injected_func.__name__ = func.__name__
        injected_func.__doc__ = func.__doc__

        return injected_func

    def get_source_reader(self):
        """
        """
        func = get_reader(self.type.name)

        def injected_func(entry, **kwargs):
            return func(entry, self, **kwargs)
        injected_func.__name__ = func.__name__
        injected_func.__doc__ = func.__doc__

        return injected_func

    def __str__(self):
        return "%s data source at %s <ID=%d>" % (self.type.name, self.path, self.id)

