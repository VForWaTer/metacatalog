import pandas as pd
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, DateTime, Numeric

from metacatalog.db.base import Base


class TimeseriesPoint(Base):
    __tablename__ = 'timeseries'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    tstamp = Column(DateTime, primary_key=True)
    value = Column(Numeric, nullable=False)
    precision = Column(Numeric, nullable=True)

    @classmethod
    def is_valid_timeseries(cls, data):
        return isinstance(data, (pd.DataFrame, pd.Series)) and isinstance(data.index, pd.DatetimeIndex)


class TimeseriesPoint2D(Base):
    __tablename__ = 'timeseries_2d'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    tstamp = Column(DateTime, primary_key=True)
    value1 = Column(Numeric, nullable=False)
    value2 = Column(Numeric, nullable=False)
    precision1 = Column(Numeric, nullable=True)
    precision2 = Column(Numeric, nullable=True)

    @classmethod
    def is_valid_timeseries(cls, data):
        return isinstance(data, pd.DataFrame) and isinstance(data.index, pd.DatetimeIndex) and len(data.columns) == 2