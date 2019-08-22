from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, DateTime, Numeric

from metacatalog.db import Base


class TimeseriesPoint(Base):
    __tablename__ = 'timeseries'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    tstamp = Column(DateTime, primary_key=True)
    value = Column(Numeric, nullable=False)
