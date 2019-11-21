import json
from functools import wraps

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, object_session

from metacatalog.db.base import Base
from metacatalog.util.exceptions import NoImporterFoundWarning, NoReaderFoundWarning
from metacatalog.util.importer import (
    import_to_interal_table,
    import_to_local_csv_file
)
from metacatalog.util.reader import (
    read_from_interal_table,
    read_from_local_csv_file
)


class DataSourceType(Base):
    __tablename__ = 'datasource_types'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    description = Column(String)

    # relationships
    sources = relationship("DataSource", back_populates='type')


class DataSource(Base):
    __tablename__ = 'datasources'

    # column
    id = Column(Integer, primary_key=True)
    type_id = Column(Integer, ForeignKey('datasource_types.id'), nullable=False)
    path = Column(String, nullable=False)
    args = Column(String)

    # relationships
    entries = relationship("Entry", back_populates='datasource')
    type =relationship("DataSourceType", back_populates='sources')

    @classmethod
    def is_valid(cls, ds):
        return hasattr(ds, 'path') and isinstance(ds, DataSource)

    def parse_args(self):
        """Load args

        Load the contents of the args column as assumed JSON string.
        This will be passed to the importer/adder function as **kwargs.
        Therefore this is only useful for a DB admin and should not be 
        exposed to the user.

        """
        # return empty dict
        if self.args is None:
            return dict()

        # parse and return
        else:
            return json.loads(self.args)
    
    def save_args_from_dict(self, args_dict, commit=False):
        """Save to args

        Save all given keyword arguments to the database. These are passed 
        to the importer/adder functions as **kwargs.

        Note
        ----
        All kwargs need to be json encodeable

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
        if self.type.name == 'internal':
            func = import_to_interal_table
        elif self.type.name == 'csv':
            func = import_to_local_csv_file
        else:
            raise NoImporterFoundWarning('Method %s not supported' % self.type.name)

        def injected_func(entry, timeseries, **kwargs):
            return func(entry, timeseries, self, **kwargs)
        injected_func.__name__ = func.__name__
        injected_func.__doc__ = func.__doc__

        return injected_func

    def get_source_reader(self):
        """
        """
        if self.type.name == 'internal':
            func = read_from_interal_table
        elif self.type.name == 'csv':
            func = read_from_local_csv_file
        else:
            raise NoReaderFoundWarning('Method %s not supported' % self.type.name)

        def injected_func(entry, **kwargs):
            return func(entry, self, **kwargs)
        injected_func.__name__ = func.__name__
        injected_func.__doc__ = func.__doc__

        return injected_func

    def __str__(self):
        return "%s data source at %s <ID=%d>" % (self.type.name, self.path, self.id)

