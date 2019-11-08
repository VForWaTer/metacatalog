from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd

from sqlalchemy import Column, ForeignKey, event
from sqlalchemy import Integer, String, Boolean, DateTime
from geoalchemy2 import Geometry
from sqlalchemy.orm import relationship, backref, object_session

from metacatalog.db import Base
from metacatalog.models.entrygroup import EntryGroupAssociation


def get_embargo_end(datetime=None):
    if datetime is None:
        datetime = dt.utcnow()
    return datetime + rd(years=2)


class Entry(Base):
    __tablename__ = 'entries'

    # columns
    id = Column(Integer, primary_key=True)
    title = Column(String(512), nullable=False)
    abstract = Column(String)
    external_id = Column(String)
    location = Column(Geometry('POINT'), nullable=False)
    geom = Column(Geometry)
    creation = Column(DateTime)
    end = Column(DateTime) # TODO: nachschalgen
    version = Column(Integer, default=1, nullable=False)
    latest_version_id = Column(Integer, ForeignKey('entries.id'), nullable=True)
    
    license_id = Column(Integer, ForeignKey('licenses.id'))
    variable_id = Column(Integer, ForeignKey('variables.id'), nullable=False)
    datasource_id = Column(Integer, ForeignKey('datasources.id'))

    embargo = Column(Boolean, default=False, nullable=False)
    embargo_end = Column(DateTime, default=get_embargo_end)

    publication = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    contributors = relationship("PersonAssociation", back_populates='entry')
    keywords = relationship("KeywordAssociation", back_populates='entry')
    license = relationship("License", back_populates='entries')
    variable = relationship("Variable", back_populates='entries')
    datasource = relationship("DataSource", back_populates='entries')
    other_versions = relationship("Entry", backref=backref('latest_version', remote_side=[id]))
    associated_groups = relationship("EntryGroup", secondary="nm_entrygroups", back_populates='entries')

    @property
    def is_latest_version(self):
        self.latest_version_id == self.id

    @property
    def projects(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'project']

    @property
    def composite_entries(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'composite']
    
    def plain_keywords_list(self):
        """Metadata Keyword list

        Returns list of controlled keywords associated with this 
        instance of meta data. 
        If there are any associated values or alias of the given 
        keywords, use the keywords_dict function

        """
        return [kw.keyword.path() for kw in self.keywords]
    
    def plain_keywords_dict(self):
        return [kw.keyword.as_dict() for kw in self.keywords]
    
    def keywords_dict(self):
        return [
            dict(
                path=kw.keyword.full_path, 
                alias=kw.alias,
                value=kw.associated_value
            ) for kw in self.keywords
        ]

    def __str__(self):
        return "<ID=%d %s [%s] >" % (
            self.id, 
            self.title[:20], 
            self.variable.name
            )


#@event.listens_for(Entry, 'after_insert')
#def insert_event_latest_version(mapper, connection, entry):
#   """
#    If entry does not reference a latest version it should 
#    reference itself to mark itself being up to date.
#    """
#    if entry.latest_version_id is None:
#        entry.latest_version_id = entry.id
#    
#    # make changes
#    session = object_session(entry)
#    session.add(entry)
#    session.commit()
