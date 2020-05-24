"""
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
from geoalchemy2.shape import to_shape, from_shape
from sqlalchemy.orm import relationship, backref, object_session

import nltk
import pandas as pd
import numpy as np

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
    r"""Entry

    The Entry is the main entity in metacatalog. An object instance models a 
    set of metadata needed to store and manage a datasource. The Entry is not 
    the actual data. 
    The Entry is designed to store all necessary information to be exportable 
    in ISO19115 in the scope of metacatalog. That means, Properties which are 
    always the same across metacatalog, or can be derived from the actual 
    implementation, are not part of an Entry.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    title : str
        A full title (512) to describe the datasource as well as possible.
        The truncated title (first 25 signs) is usually used to print an 
        Entry object to the console.
    abstract : str
        Full abstract of the datasource. The abstract should include all 
        necessary information that is needed to fully understand the data. 
    external_id : str
        Any kind of OID that was used to identify the data in the first place. 
        Usually an unque ID field of other data-storage solutions. The 
        exernal_id is only stored for reference reasons.       
    location : str, tuple
        The location as a POINT Geometry in unprojected WGS84 (EPSG: 4326).
        The location is primarily used to show all Entry objects on a map, or 
        perform geo-searches. If the data-source needs to store more complex 
        Geometries, you can use the ``geom`` argument.
        The location can be passed as WKT or a tuple of (x, y) coordinates.
        Note that it will be returned and stored as WKB. The output value will 
        be reworked in a future release
    geom : str
        .. warning::
            The geom attribute is completely untested so far and might be 
            reworked or removed in a future release
            It takes a WKT of any kind of OGC-conform Geometry. The return value 
            will be the same Geometry as WKB.
    creation : datetime.datetime
        Following the ISO19115 the *creation* date is referring to the creation 
        date of the **data resource** described by the Entry, not the Entry 
        itself. If creation date is not set, it is assumed, that yet no data 
        resource is connected to the Entry.
    end : datetime.datimetime
        The last date the data source described by this Entry has data for. 
        The end date is **not** ISO19115-compliant and will be reworked.
    version : int
        The version of this Entry. Usually metacatalog will handle the version 
        itself and there is not need to set the version manually.
    latest_version_id : int
        Foreign key to `Entry.id`. This key is self-referencing the another 
        Entry. This has to be set if the current Entry is not the latest one. 
        If latest_version_id is None, the Entry is the most recent one and 
        database operations that find multiple entries will in a future release 
        filter to 'version duplicates'.
    comment : str
        Arbitrary free-text comment to the Entry
    license : metacatalog.models.License
        Data License associated to the data and the metadata. You can pass 
        the `License <metacatalog.models.License>`_ itself, or use the 
        license_id attribute.
    license_id : int
        Foreign key to the data license. 
    author : metacatalog.models.Person
        :class:`Person <metacatalog.models.Person>` that acts as first author 
        for the given entry. Only one first author is possible, co-authors can 
        be requested from either the contributors list or the 
        :py:attr:`authors` property. `author` is a property and setting a 
        new author using this property is not supported.
    authors : list
        List of :class:`Person <metacatalog.models.Person>`. The first element 
        is the first author, see :py:attr:`~author`. The others are 
        :class:`Person <metacatalog.models.Person>`s associated with the 
        :class:`Role <metacatalog.models.PersonRole>` of ``'coAuthor' ``. 
        The list of authors is sorted by the `order` attribute.
        `authors` is a property and setting a new list of authors using this 
        property is not supported.

    Note
    ----
    One Entry object instance is always described by exactly one variable. 
    If a datasource is a composite of many datasources, there are two 
    strategies. Either a new table can be implemented and an abstract 
    :class:`Variable <metacatalog.models.Variable>` be added. This is done with 
    Eddy-Covariance data. Secondly, Each variable of the datasource can be 
    represented by its own Entry, which get then grouped by an 
    :class:`EntryGroup` of :class:`EntryGroupType` ``'composite'``.

    See Also
    --------
    `EntryGroup`
    `EntryGroupType
    """
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
    comment = Column(String, nullable=True)
    
    license_id = Column(Integer, ForeignKey('licenses.id'))
    variable_id = Column(Integer, ForeignKey('variables.id'), nullable=False)
    datasource_id = Column(Integer, ForeignKey('datasources.id'))

    embargo = Column(Boolean, default=False, nullable=False)
    embargo_end = Column(DateTime, default=get_embargo_end)

    publication = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    contributors = relationship("PersonAssociation", back_populates='entry', cascade='all, delete, delete-orphan')
    keywords = relationship("KeywordAssociation", back_populates='entry', cascade='all, delete, delete-orphan')
    license = relationship("License", back_populates='entries')
    variable = relationship("Variable", back_populates='entries')
    datasource = relationship("DataSource", back_populates='entries', cascade='all, delete, delete-orphan', single_parent=True)
    other_versions = relationship("Entry", backref=backref('latest_version', remote_side=[id]))
    associated_groups = relationship("EntryGroup", secondary="nm_entrygroups", back_populates='entries')
    details = relationship("Detail", back_populates='entry')

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
            id=self.id,
            title=self.title,
            author=self.author,
            authors=self.authors,
            locationShape=self.location_shape.wkt,
            location=self.location_shape.wkt,
            variable=self.variable.to_dict(deep=False),
            embargo=self.embargo,
            embargo_end=self.embargo_end,
            version=self.version,
            publication=self.publication,
            lastUpdate=self.lastUpdate,
            keywords=self.plain_keywords_dict()
        )

        if self.license is not None:
            d['license'] = self.license.to_dict(deep=False)

        if self.details is not None:
            d['details'] = self.details_dict(full=True)
        
        # set optional attributes
        for attr in ('abstract', 'external_id','creation','end','comment'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # lazy loading
        if deep:
            projects = self.projects
            if len(projects) > 0:
                d['projects'] = [p.to_dict(deep=False) for p in projects]
            comp = self.composite_entries
            if len(comp) > 0:
                d['composite_entries'] = [e.to_dict(deep=False) for e in comp]

        return d

    @classmethod
    def is_valid(cls, entry):
        return isinstance(entry, Entry) and entry.id is not None

    @property
    def is_latest_version(self):
        self.latest_version_id == self.id

    @property
    def author(self):
        return [c.person for c in self.contributors if c.role.name == 'author'][0]
    
    @property
    def authors(self):
        # get all
        coAuthors = [c for c in self.contributors if c.role.name == 'coAuthor']
        
        # order
        idx = np.argsort([c.order for c in coAuthors])

        # build the author list
        authors = [self.author]
        for i in idx:
            authors.append(coAuthors[i].person)

        return authors

    @property
    def projects(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'project']

    @property
    def composite_entries(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'composite']
    
    @property
    def location_shape(self):
        return to_shape(self.location)

    @location_shape.setter
    def location_shape(self, shape):
        self.location = from_shape(shape)
    
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

    def details_dict(self, full=True):
        """
        Returns the associated details as dictionary.
        
        Parameters
        ----------
        full : bool
            If True (default) the keywords will contain the 
            full info including key description, ids and 
            stemmed key. If false, it will be truncated to a
            plain key:value dict 

        """
        if full:
            return {d.stem:d.to_dict() for d in self.details}
        else:
            return {d.stem:d.value for d in self.details}
    
    def details_table(self, fmt='html'):
        """
        Return the associated details as table

        Parameters
        ----------
        fmt : string
            Can be one of:
            
            * `html` to return a HTML table
            * `latex` to return LaTeX table
            * `markdown` to return Markdown table

        """
        # get the details
        df = pd.DataFrame(self.details_dict(full=True)).T

        if fmt.lower() == 'html':
            return df.to_html()
        elif fmt.lower() == 'latex':
            return df.to_latex()
        elif fmt.lower() == 'markdown' or fmt.lower() == 'md':
            return df.to_markdown()
        else:
            raise ValueError("fmt has to be in ['html', 'latex', 'markdown']")
    
    def add_details(self, commit=False, **kwargs):
        """
        Adds arbitrary key-value pairs to this entry.

        Parameters
        ----------
        commit : bool
            If True, the Entry session will be added to the 
            current session and the transaction is commited.
            Can have side-effects. Defaults to False.
        
        """
        ps = nltk.PorterStemmer()
        for k, v in kwargs.items():
            self.details.append(models.Detail(**{'entry_id': self.id, 'key': str(k), 'stem': ps.stem(k), 'value': str(v)}))
        
        if commit:
            session = object_session(self)
            try:
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

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
