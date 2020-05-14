import pandas as pd
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Numeric

from metacatalog.db.base import Base


class DataPoint(Base):
    __tablename__ = 'generic_1d_data'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    index = Column(Numeric, primary_key=True)
    value = Column(Numeric, nullable=False)
    precision = Column(Numeric, nullable=True)

    @classmethod
    def is_valid_data_series(cls, data):
        return isinstance(data, (pd.DataFrame, pd.Series)) and len(data.columns)


class DataPoint2D(Base):
    __tablename__ = 'generic_2d_data'

    # columns
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    index = Column(Numeric, primary_key=True)
    value1 = Column(Numeric, nullable=False)
    value2 = Column(Numeric, nullable=False)
    precision1 = Column(Numeric, nullable=True)
    precision2 = Column(Numeric, nullable=True)

    @classmethod
    def is_valid_data_series(cls, data):
        return isinstance(data, pd.DataFrame) and len(data.columns) == 2
