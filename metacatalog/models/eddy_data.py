from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer

from metacatalog.db.base import Base


class EddyData(Base):
    __tablename__ = 'eddy_data'

    # columns
    entry_id  = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    
    # TODO: put all the other columns here