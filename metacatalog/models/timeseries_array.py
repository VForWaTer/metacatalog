import pandas as pd
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, DateTime, Numeric, ARRAY

from metacatalog.db.base import Base


class TimeseriesArray(Base):
    __tablename__ = 'timeseries_array'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    tstamp = Column(DateTime, primary_key=True)
    data = Column(ARRAY(Numeric), nullable=False)
    precision = Column(ARRAY(Numeric), nullable=True)

    @classmethod
    def is_valid_timeseries(cls, data):
        return isinstance(data, (pd.DataFrame, pd.Series)) and isinstance(data.index, pd.DatetimeIndex)
