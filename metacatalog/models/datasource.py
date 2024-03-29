from typing import Union, List, TYPE_CHECKING
if TYPE_CHECKING:
    from metacatalog.models import Entry
import json
from functools import wraps
from datetime import datetime as dt
from datetime import timedelta

from sqlalchemy import Column, ForeignKey, CheckConstraint
from sqlalchemy import Integer, String, DateTime, Numeric
from sqlalchemy.orm import relationship, object_session, backref
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape
from sqlalchemy.dialects.postgresql import ARRAY

import pandas as pd

from metacatalog.db.base import Base
from metacatalog.util.exceptions import MetadataMissingError


class DataSourceType(Base):
    r"""
    Model to represent a type of datasource.

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
    
    Note
    ----
    While it is possible to add more records to the table,
    this is the only Class that needs actual Python functions to
    handle the database input. Usually, each type of datasource
    relies on a specific :mod:`importer <metacatalog.util.importer>`
    and reader :mod:`reader <metacatalog.util.reader>` that can use
    the information saved in a :class:`DataSource <metacatalog.models.DataSource>`
    to perform I/O operations.


    """
    __tablename__ = 'datasource_types'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    title = Column(String, nullable=False)
    description = Column(String)

    # relationships
    sources: List['DataSource'] = relationship("DataSource", back_populates='type')

    def to_dict(self, deep: bool = False) -> dict:
        """
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

    def __str__(self) -> str:
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
    sources: List['DataSource'] = relationship("DataSource", back_populates='datatype')
    children: List['DataType'] = relationship("DataType", backref=backref('parent', remote_side=[id]))

    def to_dict(self, deep: bool = False) -> dict:
        """
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

    def parent_list(self) -> List['DataType']:
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

    def children_list(self) -> List['DataType']:
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

        .. code-block:: text

            'P[n]Y[n]M[n]DT[n]H[n]M[n]S'
            
    observation_start : datetime.datetime
        Point in time, when the first observation was made.
        Forms the temporal extent toghether with `observation_end`.
    observation_end : datetime.datetime
        Point in time, when the last available observation was made.
        Forms the temporal extent toghether with `observation_start`.
    support : float
        The support gives the temporal validity for a single observation.
        It specifies the time before an observation, that is still
        represented by the observation.
        It is given as a fraction of resolution.
        I.e. if ``support=0.5`` at ``resolution='10min'``, the observation
        supports ``5min`` (5min before the timestamp) and the resulting dataset
        would **not** be exhaustive.
        Defaults to ``support=1.0``, which would make a temporal exhaustive
        dataset, but may not apply to each dataset.
    dimension_names : List[str]
        versionadded:: 0.9.1

        Name of the temporal dimension.  
        In case of tabular data, this is usually the column name of the column
        that stores the temporal information of the dataset. In case of a netCDF
        file, this is the dimension name of the dimension that stores the temporal
        information of the dataset.
        More generally, dimension_names describes how a datasource would be indexed
        to retrieve the temporal axis of the entry (e.g. 'time', 'date', 'datetime').


    """
    __tablename__ = 'temporal_scales'

    # columns
    id = Column(Integer, primary_key=True)

    resolution = Column(String, nullable=False)
    observation_start = Column(DateTime, nullable=False)
    observation_end = Column(DateTime, nullable=False)
    support = Column(Numeric, CheckConstraint('support >= 0'), nullable=False, default=1.0)
    dimension_names = Column(ARRAY(String(32)), nullable=True)

    # relationships
    sources: List['DataSource'] = relationship("DataSource", back_populates='temporal_scale')

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
    def resolution_timedelta(self) -> pd.Timedelta:
        return pd.to_timedelta(self.resolution)

    @resolution_timedelta.setter
    def resolution_timedelta(self, delta: Union[str, float, timedelta]):
        self.resolution = pd.to_timedelta(delta).isoformat()

    @property
    def support_timedelta(self) -> pd.Timedelta:
        return self.resolution_timedelta / 2

    @resolution_timedelta.setter
    def support_timedelta(self, delta: Union[str, float, timedelta]):
        self.support = pd.to_timedelta(delta) / self.resolution_timedelta

    @property
    def extent(self) -> timedelta:
        return [self.observation_start, self.observation_end]

    @extent.setter
    def extent(self, extent: List[dt]):
        self.observation_start, self.observation_end = extent

    def to_dict(self, deep: bool = False) -> dict:
        """
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

        # set optionals
        if self.dimension_names is not None:
            d['dimension_names'] = self.dimension_names

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
        The spatial extent of the dataset is given as a ``'POLYGON'``. 
        .. versionchanged:: 0.6.1
        From this ``POLYGON``, a bounding box and the centroid are internally
        calculated.
        To specify a point location here, use the same value for easting and
        westing and the same value for northing and southing.
    support : float
        The support gives the spatial validity for a single observation.
        It specifies the spatial extent at which an observed value is valid.
        It is given as a fraction of resolution. For gridded datasets, it is
        common to set support to 1, as the observations are validated to
        represent the whole grid cell. In case ground truthing data is
        available, the actual footprint fraction of observations can be
        given here.
        Defaults to ``support=1.0``.
    dimension_names : List[str]
        versionadded:: 0.9.1

        Names of the spatial dimension in x, y and optionally z-direction.  
        Put the names in a list in the order x, y(, z).
        In case of tabular data, this is usually the column name of the column
        that stores the spatial information of the dataset.  
        In case of a netCDF file, this is the dimension name of the dimension 
        that stores the spatial information of the dataset.  
        More generally, dimension_names describes how a datasource would be indexed
        to retrieve the spatial axis of the entry in x-direction 
        (e.g. ['x', 'y', 'z'], ['lon', 'lat'], ['longitude', 'latitude']).

    """
    __tablename__ = 'spatial_scales'

    # columns
    id = Column(Integer, primary_key=True)

    resolution = Column(Integer, nullable=False)
    extent = Column(Geometry(geometry_type='POLYGON', srid=4326), nullable=False)
    support = Column(Numeric, CheckConstraint('support >= 0'), nullable=False, default=1.0)
    dimension_names = Column(ARRAY(String(32)), nullable=True)

    # relationships
    sources: List['DataSource'] = relationship("DataSource", back_populates='spatial_scale')

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
        if (self.support * self.resolution) / 1000 > 1:
            return '%d km' % (int((self.support * self.resolution) / 1000))
        return '%.1f m' % (self.support * self.resolution)

    def to_dict(self, deep: bool = False) -> dict:
        """
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

        # set optionals
        if self.dimension_names is not None:
            d['dimension_names'] = self.dimension_names

        if deep:
            d['datasources'] = [s.to_dict(deep=False) for s in self.sources]

        return d


class DataSource(Base):
    r"""
    Model to represent a datasource of a specific
    :class:`Entry <metacatalog.models.Entry>`. The datasource further specifies
    an :class:`DataSourceType <metacatalog.models.DataSourceType>` 
    by setting a ``path`` and ``args``.

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
        I/O functions as ``**kwargs``.
    type_id : int
        Foreign key referencing the ::class:`DataSourceType <metacatalog.models.DataSourceType>`.
    type : metacatalog.models.DataSourceType
        The referenced :class:`DataSourceType <metacatalog.models.DataSourceType>`. 
        Can be used instead of setting``type_id``.
    data_names : list
        .. versionadded:: 0.3.0
        .. deprecated:: 0.9.1

        List of column names that will be displayed when exporting the data.
        The columns are named in the same order as they appear in the list.
    variable_names : list[str]
        .. versionadded:: 0.9.1

        List of variable names that store the data of the datasource of the entry.
        In tabular data, this is usually the column name(s) of the variable that
        is referenced by the Entry. In case of a netCDF file, this is the variable
        name(s) of the variable(s) that is/are referenced by the Entry.  
        More generally, variable_names describes how a datasource would be indexed
        to retrieve the data of the entry.

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
    data_names = Column(ARRAY(String(128)), nullable=True) # deprecated
    variable_names = Column(ARRAY(String(128)), nullable=True)
    args = Column(String)

    # scales
    temporal_scale_id = Column(Integer, ForeignKey('temporal_scales.id'))
    spatial_scale_id = Column(Integer, ForeignKey('spatial_scales.id'))

    creation = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    entries: List['Entry'] = relationship("Entry", back_populates='datasource')
    type: 'DataSourceType' = relationship("DataSourceType", back_populates='sources')
    datatype: 'DataType' = relationship("DataType", back_populates='sources')
    temporal_scale: 'TemporalScale' = relationship("TemporalScale", back_populates='sources')
    spatial_scale: 'SpatialScale' = relationship("SpatialScale", back_populates='sources')

    @classmethod
    def is_valid(cls, ds: 'DataSource') -> bool:
        return hasattr(ds, 'path') and isinstance(ds, DataSource)
    
    @property
    def dimension_names(self) -> List[str]:
        """
        .. versionadded:: 0.9.1

        Returns a flat list of all dimensions needed to identify a datapoint in the dataset.
        The order is [temporal, spatial, variable].

        Returns
        -------
        dimension_names : List[str]
            List of dimension names

        """
        # get the single dimensions
        temporal = self.temporal_scale.dimension_names if self.temporal_scale is not None else []
        spatial = self.spatial_scale.dimension_names if self.spatial_scale is not None else []
        variable = self.variable_names if self.variable_names is not None else []

        # build the flat list
        return [*temporal, *spatial, *variable]

    def to_dict(self, deep: bool = False) -> dict:
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
        if self.data_names is not None:
            d['data_names'] = self.data_names
        if self.variable_names is not None:
            d['variable_names'] = self.variable_names
        if self.args is not None:
            d['args'] = self.load_args()
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

    def load_args(self) -> dict:
        """
        Load the stored arguments from the ``'args'`` column.
        It was filled by a JSON string and will be converted as dict before.
        This dict is usually used for I/O operations and passed as keyword arguments.
        Therefore this is only useful for a DB admin and should not be exposed to the end-user.
        
        .. versionadded:: 0.1.11

        """
        # return empty dict
        if self.args is None:
            return dict()

        # parse and return
        else:
            return json.loads(self.args)

    def save_args_from_dict(self, args_dict: dict, commit: bool = False) -> None:
        """
        Save all given keyword arguments to the database.
        These are passed to the importer/adder functions as ``**kwargs``.

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
        load_args

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

    def create_scale(self, resolution, extent, support, scale_dimension, dimension_names: List[str] = None, commit: bool = False) -> None:
        """
        Create a new scale for the dataset
        """
        # get the correct class
        if scale_dimension.lower() == 'temporal':
            Cls = TemporalScale
            if self.temporal_scale is not None:
                raise MetadataMissingError('Temporal scale already exists. You can edit that one.')
        elif scale_dimension.lower() == 'spatial':
            Cls = SpatialScale
            if self.spatial_scale is not None:
                raise MetadataMissingError('Spatial scale already exists. You can edit that one.')
        else:
            raise AttributeError("scale_dimension has to be in ['temporal', 'spatial']")

        # check that dimension_names is a list
        if isinstance(dimension_names, str):
            dimension_names = [dimension_names]

        # build the scale and append
        scale = Cls(resolution=resolution, extent=extent, support=support, dimension_names=dimension_names)
        setattr(self, '%s_scale' % scale_dimension.lower(), scale)

        if commit:
            try:
                session = object_session(self)
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    def __str__(self) -> str:
        return "%s data source at %s <ID=%d>" % (self.type.name, self.path, self.id)
