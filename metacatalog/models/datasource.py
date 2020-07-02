import json
from functools import wraps
from datetime import datetime as dt

from sqlalchemy import Column, ForeignKey, CheckConstraint
from sqlalchemy import Integer, String, DateTime, Numeric
from sqlalchemy.orm import relationship, object_session, backref
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape

import pandas as pd

from metacatalog.db.base import Base
from metacatalog.ext import extension


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


class DataType(Base):
    """
    DataType is describing the type of the actual data. 
    The metacatalog documentation includes several default abstract 
    types. Each combination of 
    :class:`DataType <metacatalog.models.DataType>` and 
    :class:`DataSourceType <metacatalog.models.DataSourceType>` can be 
    assigned with custom reader and writer functions.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    name : str
        A short (64) name for the DataType. Should not contain any whitespaces.
    title : str
        The full title of this DataType.
    description : str
        Optional description about this DataType.

    """
    __tablename__ = 'datatypes'

    # columns
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('datatypes.id'), nullable=True)
    name = Column(String(64), nullable=False)
    title = Column(String, nullable=False)
    description = Column(String, nullable=True)

    # relationships
    sources = relationship("DataSource", back_populates='datatype')
    children = relationship("DataType", backref=backref('parent', remote_side=[id]))

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
        if self.parent_id is not None:
            d['parent_id'] = self.parent_id
        
        # deep loading
        if deep:
            d['sources'] = [s.to_dict(deep=True) for s in self.sources]
            if self.parent is not None:
                d['parent'] = self.parent.to_dict(deep=True)
            if self.children is not None:
                d['children'] = [c.to_dict(deep=True) for c in self.children]
        else:
            d['parents'] = [dt.to_dict(deep=False) for dt in self.parent_list()]
            d['children'] = [dt.to_dict(deep=False) for dt in self.children_list()]
        
        return d

    def parent_list(self):
        """
        Returns an inheritance tree for the current datatype.
        If the list is empty, the current datatype is a 
        top-level datatype. 
        Otherwise, the list contains all parent datatypes 
        that the current one inherits from.

        """
        parents = []

        current_parent = self.parent
        while current_parent is not None:
            parents.append(current_parent)
        
        return parents

    def children_list(self):
        """
        Returns an dependency tree for the current datatype.
        If the list is empty, there are no child (inheriting) 
        datatypes for the current datatype.
        Otherwise, the list contains all child datatypes that 
        are inheriting the current datatype.
        """
        children = []

        current_children = self.children
        while current_children is not None and len(current_children) > 1:
            children.extend(current_children)

        return children


class TemporalScale(Base):
    """
    The TemporalScale is used to commonly describe the temporal scale at which 
    the data described is valid. metacatalog uses the scale triplet 
    (spacing, extent, support), but renames ``'spacing'`` to ``'resolution'``.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    resolution : str
        Temporal resolution. The resolution has to be given as an ISO 8601 
        Duration, or a fraction of it. You can substitute standalone minutes can 
        be identified by non-ISO ``'min'``.
        .. code-block:: python
            resolution = '15min'
        defines a temporal resolution of 15 Minutes. The ISO 8601 is built like:
        .. code-block::
            'P[n]Y[n]M[n]DT[n]H[n]M[n]S'
    observation_start : datetime.datetime
        Point in time, when the first observation was made. 
        Forms the temporal extent toghether with `observation_end`.
    observation_end : datetime.datetime
        Point in time, when the last available observation was made.
        Forms the temporal extent toghether with `observation_start`.
    support : float
        The support gives the temporal validity for a single observation.
        It specifies the time before and after observation, that is still 
        represented by the observation. 
        It is given as a fraction of resolution. 
        I.e. if ``support=0.5`` at ``resolution='10min'``, the observation 
        supports ``5min`` (2.5min before and after the timestamp) and the 
        resulting dataset would **not** be exhaustive. 
        Defaults to ``support=1.0``, which would make a temporal exhaustive 
        dataset, but may not apply to each dataset. 

    """
    __tablename__ = 'temporal_scales'

    # columns
    id = Column(Integer, primary_key=True)

    resolution = Column(String, nullable=False)
    observation_start = Column(DateTime, nullable=False)
    observation_end = Column(DateTime, nullable=False)
    support = Column(Numeric, CheckConstraint('support >= 0'), nullable=False, default=1.0)

    # relationships
    sources = relationship("DataSource", back_populates='temporal_scale')

    def __init__(self, *args, **kwargs):
        # handle resoultion 
        if 'resolution_timedelta' in kwargs:
            kwargs['resolution'] = pd.to_timedelta(kwargs['resolution_timedelta']).isoformat()
            del kwargs['resolution_timedelta']
        if 'extent' in kwargs:
            kwargs['observation_start'] = kwargs['extent'][0]
            kwargs['observation_end'] = kwargs['extent'][1]
            del kwargs['extent']
        if 'resolution' in kwargs:
            kwargs['resolution'] = pd.to_timedelta(kwargs['resolution']).isoformat()
        super(TemporalScale, self).__init__(*args, **kwargs)
    
    @property 
    def resolution_timedelta(self):
        return pd.to_timedelta(self.resolution)

    @resolution_timedelta.setter
    def resolution_timedelta(self, delta):
        self.resolution = pd.to_timedelta(delta).isoformat()

    @property
    def support_timedelta(self):
        return self.resolution_timedelta / 2

    @resolution_timedelta.setter
    def support_timedelta(self, delta):
        self.support = pd.to_timedelta(delta) / self.resolution_timedelta

    @property
    def extent(self):
        return [self.observation_start, self.observation_end]
    
    @extent.setter
    def extent(self, extent):
        self.observation_start, self.observation_end = extent

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
            resolution=self.resolution,
            extent=self.extent,
            support=self.support,
            support_iso = self.support_timedelta.isoformat()
        )

        if deep:
            d['datasources'] = [s.to_dict(deep=False) for s in self.sources]
        
        return d


class SpatialScale(Base):
    """
    The SpatialScale is used to commonly describe the spatial scale at which 
    the data described is valid. metacatalog uses the scale triplet 
    (spacing, extent, support), but renames ``'spacing'`` to ``'resolution'``.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    resolution : int
        Spatial resoultion in meter. The resolution usually describes a grid
        cell size, which only applies to gridded datasets. Use the 
        :attr:`resolution_str` property for a string representation
    extent : geoalchemy2.Geometry
        The spatial extent of the dataset is given as a ``'POLYGON'``. While 
        metacatalog is capable of storing any kind of valid POLYGON as extent, 
        it is best practice to allow only Bounding Boxes on upload.
    support : float
        The support gives the spatial validity for a single observation.
        It specifies the spatial extent at which an observed value is valid.
        It is given as a fraction of resolution. For gridded datasets, it is 
        common to set support to 1, as the observations are validated to 
        represent the whole grid cell. In case ground truthing data is 
        available, the actual footprint fraction of observations can be 
        given here. 
        Defaults to ``support=1.0``. 
    
    """
    __tablename__ = 'spatial_scales'

    # columns
    id = Column(Integer, primary_key=True)

    resolution = Column(Integer, nullable=False)
    extent = Column(Geometry(geometry_type='POLYGON', srid=4326), nullable=False)
    support = Column(Numeric, CheckConstraint('support >= 0'), nullable=False, default=1.0)

    # relationships
    sources = relationship("DataSource", back_populates='spatial_scale')

    @property
    def extent_shape(self):
        return to_shape(self.extent)

    @extent_shape.setter
    def extent_shape(self, shape):
        self.extent = from_shape(shape)

    @property
    def resolution_str(self):
        if self.resolution / 1000 > 1:
            return '%d km' % (int(self.resolution / 1000))
        return '%.1f m' % self.resolution
    
    @property
    def support_str(self):
        if (self.support * self.resultion) / 1000 > 1:
            return '%d km' % (int((self.support * self.resultion) / 1000))
        return '%.1f m' % (self.support * self.resolution)

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
            resolution=self.resolution,
            resolution_str=self.resolution_str,
            extent=self.extent_shape.wkt,
            support=self.support,
            support_str = self.support_str
        )

        if deep:
            d['datasources'] = [s.to_dict(deep=False) for s in self.sources]
        
        return d


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
    encoding : str
        The encoding of the file or database representation of the actual 
        data. Defaults to ``'utf-8'``. Do only change if necessary.
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
    datatype_id = Column(Integer, ForeignKey('datatypes.id'), nullable=False)
    encoding = Column(String(64), default='utf-8')
    path = Column(String, nullable=False)
    args = Column(String)

    # scales
    temporal_scale_id = Column(Integer, ForeignKey('temporal_scales.id'))
    spatial_scale_id = Column(Integer, ForeignKey('spatial_scales.id'))

    creation = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    entries = relationship("Entry", back_populates='datasource')
    type = relationship("DataSourceType", back_populates='sources')
    datatype = relationship("DataType", back_populates='sources')
    temporal_scale = relationship("TemporalScale", back_populates='sources')
    spatial_scale = relationship("SpatialScale", back_populates='sources')

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
            type=self.type.to_dict(deep=False),
            creation=self.creation,
            lastUpdate=self.lastUpdate
        )

        # set optionals
        if self.args is not None:
            d['args'] = self.parse_args()
        if self.encoding is not None:
            d['encoding'] = self.encoding
        if self.temporal_scale is not None:
            d['temporal_scale'] = self.temporal_scale.to_dict(deep=False)
        if self.spatial_scale is not None:
            d['spatial_scale'] = self.spatial_scale.to_dict(deep=False)
        
        # deep loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        
        return d

    def parse_args(self):
        r"""Load args
        .. depreacted:: 0.1.11
            use load_args instead

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
    
    def load_args(self) -> dict:
        """
        .. versionadded:: 0.1.11
        
        Load the stored arguments from the ``'args'`` column.
        It was filled by a JSON string and will be converted as 
        dict before. 
        This dict is usually used for I/O operations and passed 
        as keyword arguments.
        Therefore this is only useful for a DB admin and should not be 
        exposed to the end-user.

        """
        return self.parse_args()
    
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

    def create_scale(self, resolution, extent, support, scale_dimension):
        """
        Create a new scale for the dataset
        """
        # get the correct class
        if scale_dimension.lower() == 'temporal':
            Cls = TemporalScale
        elif scale_dimension.lower() == 'spatial':
            Cls = SpatialScale
        else:
            raise AttributeError("scale_dimension has to be in ['temporal', 'spatial']")
        
        # build the scale and append
        scale = Cls(resolution=resolution, extent=extent, support=support)
        setattr(self, '%s_scale' % scale_dimension.lower(), scale)

    def get_source_importer(self):
        """
        .. deprecated:: 0.1.12
            Will be removed with version 0.2

        This function is usually called by a 
        :class:`Entry <metacatalog.Entry>` object. It returns a function 
        that will import the data into the correct source.

        """
        IOExt = extension('io')
        func = IOExt.get_importer(self)

        def injected_func(entry, timeseries, **kwargs):
            return func(entry, timeseries, self, **kwargs)
        injected_func.__name__ = func.__name__
        injected_func.__doc__ = func.__doc__

        return injected_func

    def get_source_reader(self):
        """
        .. deprecated:: 0.1.12
            Will be removed with version 0.2
        """
        IOExt = extension('io')
        func = IOExt.get_reader(self)

        def injected_func(entry, **kwargs):
            return func(entry, self, **kwargs)
        injected_func.__name__ = func.__name__
        injected_func.__doc__ = func.__doc__

        return injected_func

    def __str__(self):
        return "%s data source at %s <ID=%d>" % (self.type.name, self.path, self.id)
