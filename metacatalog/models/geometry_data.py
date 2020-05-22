from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, DateTime
from geoalchemy2 import Geometry

from metacatalog.db.base import Base


class GeometryTimeseries(Base):
    __tablename__ = 'geom_timeseries'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    tstamp = Column(DateTime, primary_key=True)
    geom = Column(Geometry, nullable=False)
    srid = Column(Integer)


class GenericGeometryData(Base):
    __tablename__ = 'generic_geometry_data'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    index = Column(Integer, primary_key=True)
    geom = Column(Geometry, nullable=False)
    srid = Column(Integer)
