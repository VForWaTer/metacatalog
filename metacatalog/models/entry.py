"""
Entry
-----

The Entry is the core class of metacatalog. It represents the core logical unit of the meta data model.
In principle, an Entry needs a first Author, a title, position and a license to describe 
one type of environmental variable. It can hold a reference and interface to the actual data.
If a supported data format is used, Entry can load the data.

"""
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd

from sqlalchemy import Column, ForeignKey, event
from sqlalchemy import Integer, String, Boolean, DateTime
from geoalchemy2 import Geometry
from sqlalchemy.orm import relationship, backref, object_session

from metacatalog.db.base import Base
from metacatalog import models
from metacatalog import api
from metacatalog.util.exceptions import (
    MetadataMissingError,
    NoImporterFoundWarning,
    NoReaderFoundWarning
)


def get_embargo_end(datetime=None):
    if datetime is None:
        datetime = dt.utcnow()
    return datetime + rd(years=2)


class Entry(Base):
    __tablename__ = 'entries'

    # columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(512), nullable=False)
    abstract = Column(String)
    external_id = Column(String)
    location = Column(Geometry('POINT'), nullable=False)
    geom = Column(Geometry)
    creation = Column(DateTime)
    end = Column(DateTime) # TODO: nachschalgen
    version = Column(Integer, default=1, nullable=False)
    latest_version_id = Column(Integer, ForeignKey('entries.id'), nullable=True)
    
    license_id = Column(Integer, ForeignKey('licenses.id'))
    variable_id = Column(Integer, ForeignKey('variables.id'), nullable=False)
    datasource_id = Column(Integer, ForeignKey('datasources.id'))

    embargo = Column(Boolean, default=False, nullable=False)
    embargo_end = Column(DateTime, default=get_embargo_end)

    publication = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    contributors = relationship("PersonAssociation", back_populates='entry')
    keywords = relationship("KeywordAssociation", back_populates='entry')
    license = relationship("License", back_populates='entries')
    variable = relationship("Variable", back_populates='entries')
    datasource = relationship("DataSource", back_populates='entries')
    other_versions = relationship("Entry", backref=backref('latest_version', remote_side=[id]))
    associated_groups = relationship("EntryGroup", secondary="nm_entrygroups", back_populates='entries')

    @classmethod
    def is_valid(cls, entry):
        return isinstance(entry, Entry) and entry.id is not None

    @property
    def is_latest_version(self):
        self.latest_version_id == self.id

    @property
    def projects(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'project']

    @property
    def composite_entries(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'composite']
    
    def plain_keywords_list(self):
        """Metadata Keyword list

        Returns list of controlled keywords associated with this 
        instance of meta data. 
        If there are any associated values or alias of the given 
        keywords, use the keywords_dict function

        """
        return [kw.keyword.path() for kw in self.keywords]
    
    def plain_keywords_dict(self):
        return [kw.keyword.as_dict() for kw in self.keywords]
    
    def keywords_dict(self):
        return [
            dict(
                path=kw.keyword.full_path, 
                alias=kw.alias,
                value=kw.associated_value
            ) for kw in self.keywords
        ]

    def create_datasource(self, path, type, commit=False, **args):
        """
        """
        # 
        if self.datasource is not None:
            raise MetadataMissingError('Datasource already exists. You can edit that one.')

        # get a session
        session = object_session(self)

        # load the datasource type
        if isinstance(type, int):
            ds_type = api.find_datasource_type(session=session, id=type, return_iterator=True).one()
        elif isinstance(type, str):
            ds_type = api.find_datasource_type(session=session, name=type, return_iterator=True).first()
        else:
            raise AttributeError('type has to be of type int or str')
        
        # build the datasource object
        ds = models.DataSource(type=ds_type, path=path)

        # add the args
        ds.save_args_from_dict(args)

        # append to self
        self.datasource = ds

        if commit:
            try:
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
        
        # return
        return ds

    def get_data(self, **kwargs):
        """
        """
        if self.datasource is None:
            raise MetadataMissingError('Entry need datasource information')
            
        try:
            reader = self.datasource.get_source_reader()
        except NoReaderFoundWarning as w:
            print('[WARNING] %s' % str(w))
        
        # get the args and update with kwargs
        args = self.datasource.parse_args()
        args.update(kwargs)

        # use the reader and return
        return reader(self, **args)

    def import_data(self, data, **kwargs):
        """
        """
        if self.datasource is None:
            raise MetadataMissingError('Entry need datasource information')

        try:
            importer = self.datasource.get_source_importer()
        except NoImporterFoundWarning as w:
            print('[WARNING] %s' % str(w))
            return 
        
        # get the args and update with kwargs
        args = self.datasource.parse_args()
        args.update(kwargs)

        # import the data 
        importer(self, data, **args)


    def add_data(self):
        """
        """
        raise NotImplementedError

    def __str__(self):
        return "<ID=%d %s [%s] >" % (
            self.id, 
            self.title[:20], 
            self.variable.name
            )


#@event.listens_for(Entry, 'after_insert')
#def insert_event_latest_version(mapper, connection, entry):
#   """
#    If entry does not reference a latest version it should 
#    reference itself to mark itself being up to date.
#    """
#    if entry.latest_version_id is None:
#        entry.latest_version_id = entry.id
#    
#    # make changes
#    session = object_session(entry)
#    session.add(entry)
#    session.commit()
