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

    @classmethod
    def is_valid_timeseries(cls, data):
        return isinstance(data, (pd.DataFrame, pd.Series)) and isinstance(data.index, pd.DatetimeIndex)