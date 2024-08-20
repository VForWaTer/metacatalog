"""
The combined module is a Pydantic model of the entire datamodel.

This is considered an intermediate step to move from pydantic and SQLAlchemy models to
SQLModel in a future version. You can use this model to serialize and deserialize the
the metacata without the PostgreSQL database from JSON dumps.

"""
from typing import Optional, TYPE_CHECKING, Dict, List, Literal, Tuple
from datetime import datetime, timedelta
from enum import Enum
import json

from pydantic import BaseModel
from pydantic import UUID4, computed_field
from pydantic_geojson import PointModel

if TYPE_CHECKING:
    from metacatalog.models.entry import Entry

class Unit(BaseModel):
    id: int
    name: str
    symbol: str

class Thesaurus(BaseModel):
    id: int
    uuid: str
    name: str
    title: str
    organisation: str
    url: str
    description: str

class Keyword(BaseModel):
    id: int
    uuid: str
    value: str
    path: str
    children: List[str]
    thesaurusName: Thesaurus

class Variable(BaseModel):
    id: int
    name: str
    symbol: str
    unit: Unit
    column_names: List[str]
    keyword: Optional[Keyword] = None

class License(BaseModel):
    id: int
    short_title: str
    title: str
    by_attribution: Optional[bool] = None
    share_alike: Optional[bool] = None
    commercial_use: Optional[bool] = None
    summary: str
    link: str

class Detail(BaseModel):
    id: Optional[int] = None 
    name: Optional[str] = None
    value: Optional[str | float | bool | dict] = None
    type: Optional[Literal['string']] = None

class DataSourceNames(Enum):
    internal = 'internal'
    external = 'external'
    csv = 'csv'
    local = 'local'
    netCDF = 'netCDF'

class DataSourceType(BaseModel):
    id: Optional[int] = None
    name: DataSourceNames
    title: str
    description: Optional[str] = None

class TemporalScale(BaseModel):
    id: Optional[int] = None
    dimension_names: List[str] = []
    extent: Tuple[datetime, datetime]
    resolution: timedelta
    support: float

    @computed_field
    def support_iso(self) -> timedelta:
        return self.resolution * self.support
 
class SpatialScale(BaseModel):
    id: Optional[int] = None
    dimension_names: List[str] = []
    extent: Optional[str] = None
    resolution: Optional[int] = None
    resolution_str: Optional[str] = None
    support: Optional[float] = None
    support_str: Optional[str] = None

class DataSource(BaseModel):
    id: Optional[int] = None
    path: str
    type: Optional[DataSourceType] = None
    variable_names: List[str] = []
    temporal_scale: Optional[TemporalScale] = None
    spatial_scale: Optional[SpatialScale] = None
    args: Dict = {}
    encoding: Optional[str] = 'utf-8'

    class Config:
        use_enum_values = True

class Author(BaseModel):
    id: Optional[int] = None
    uuid: Optional[UUID4] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_organisation: Optional[bool] = None
    affiliation: Optional[str] = None
    attribution: Optional[str] = None
    organisation_name: Optional[str] = None
    organisation_abbrev: Optional[str] = None

class Location(BaseModel):
    wkt: str

    @property
    def geojson(self):
        return 
    
    @geojson.setter
    def geojson(self, value):
        pass

class Metadata(BaseModel):
    id: int
    uuid: UUID4
    title: str
    abstract: str
    external_id: Optional[str] = None
    location: Optional[Location] = None
    locationShape: Optional[Location] = None
    version: int = 1
    license: License
    latest_version_id: Optional[int] = None
    is_partial: bool = False
    comment: Optional[str] = None
    citation: Optional[str] = None
    embargo: bool = False
    embargo_end: Optional[datetime] = None
    publication: Optional[datetime] = None
    lastUpdate: Optional[datetime] = None

    author: Author
    authors: List[Author] = []
    variable: Variable
    datasource: Optional[DataSource] = None
    details: Dict[str, Detail] = {}
    keywords: List[Keyword] = []


def from_entry(entry: 'Entry') -> Metadata:
    """
    Transform an SQLAlchemy Entry instance to the Metadata model.
    """
    # get the variale
    entry_dict = entry.to_dict(deep=True, stringify=False)

    return Metadata(**entry_dict)


def from_file(file_path: str) -> Metadata:
    """
    Load a metadata file from a file path and return the Metadata model.
    """
    # open the file and parse
    with open(file_path, 'rb') as f:
        json_data = json.load(f)
    
    return Metadata(**json_data)

