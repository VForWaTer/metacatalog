from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String, Boolean, DateTime
from geoalchemy2 import Geometry
from sqlalchemy.orm import relationship

from metacatalog.db import Base


def get_embargo_end(datetime=None):
    if datetime is None:
        datetime = dt.utcnow()
    return datetime + rd(years=2)


class Entry(Base):
    __tablename__ = 'entries'

    # columns
    id = Column(Integer, primary_key=True)
    title = Column(String(512), nullable=False)
    abstract = Column(String())
    external_id = Column(String)
    location = Column(Geometry('POINT'), nullable=False)
    geom = Column(Geometry)
    start = Column(DateTime)
    end = Column(DateTime)
    
    license_id = Column(Integer, ForeignKey('licenses.id'))
    variable_id = Column(Integer, ForeignKey('variables.id'), nullable=False)
    datasource_id = Column(Integer, ForeignKey('datasources.id'))

    embargo = Column(Boolean, default=False, nullable=False)
    embargo_end = Column(DateTime, default=get_embargo_end)

    created = Column(DateTime, default=dt.utcnow)
    edited = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    contributors = relationship("PersonAssociation", back_populates='entry')
    keywords = relationship("KeywordAssociation", back_populates='entry')
    license = relationship("License", back_populates='entries')
    variable = relationship("Variable", back_populates='entries')
    datasource = relationship("DataSource", back_populates='entries')

    def keywords_list(self):
        """Metadata Keyword list

        Returns list of controlled keywords associated with this 
        instance of meta data. 
        If there are any associated values or alias of the given 
        keywords, use the keywords_dict function

        """
        return [keyword.path for keyword in self.keywords]
    
    def keywords_dict(self):
        return [kw.as_dict() for kw in self.keywords]

    def __str__(self):
        return "<ID=%d %s [%s] >" % (
            self.id, 
            self.title[:20], 
            self.variable.name
            )