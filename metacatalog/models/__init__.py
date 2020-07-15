"""
The metacatalog meta data model is split up into different 
entities, each representend by a Python class. 
Metacatalog uses sqlalchemy to model relationships between the 
classes and create and populate an appropriate database instance
to store Records of these entities.

Note
----
Due to usage of the geoalchemy2 extension, which can currently only 
be stored in a PostGIS enabled PostgreSQL database, only PostgreSQL
is supported. This may change in a future version.

"""
from .entry import Entry
from .entrygroup import EntryGroup, EntryGroupType, EntryGroupAssociation
from .details import Detail
from .keyword import Keyword, KeywordAssociation, Thesaurus
from .person import Person, PersonRole, PersonAssociation
from .license import License
from .variable import Variable, Unit
from .datasource import DataSource, DataSourceType, DataType, SpatialScale, TemporalScale
from .timeseries import TimeseriesPoint, TimeseriesPoint2D
from .generic_data import DataPoint, DataPoint2D
from .geometry_data import GeometryTimeseries, GenericGeometryData
from .config import Log, LogCodes
