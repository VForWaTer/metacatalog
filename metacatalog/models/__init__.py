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
from .keyword import Keyword, KeywordAssociation
from .person import Person, PersonRole, PersonAssociation
from .license import License
from .variable import Variable, Unit
from .datasource import DataSource, DataSourceType
from .timeseries import TimeseriesPoint
from .entrygroup import EntryGroup, EntryGroupType