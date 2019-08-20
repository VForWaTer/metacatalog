from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship

from metacatalog.db import Base


class DataSourceType(Base):
    __tablename__ = 'datasource_types'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    description = Column(String)

    # relationships
    sources = relationship("DataSource", back_populates='type')


class DataSource(Base):
    __tablename__ = 'datasources'

    # column
    id = Column(Integer, primary_key=True)
    type_id = Column(Integer, ForeignKey('datasource_types.id'), nullable=False)
    path = Column(String, nullable=False)
    args = Column(String)

    # relationships
    entries = relationship("Entry", back_populates='datasource')
    type =relationship("DataSourceType", back_populates='sources')

    def __str__(self):
        return "%s data source at %s <ID=%d>" % (self.type.name, self.path, self.id)
