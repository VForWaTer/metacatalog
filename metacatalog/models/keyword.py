from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, backref

from metacatalog.db import Base


class Keyword(Base):
    __tablename__ = 'keywords'

    # columns
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('keywords.id'))
    value = Column(String(1024))
    uuid = Column(String(64), unique=True)

    # relationships
    children = relationship("Keyword", backref=backref('parent', remote_side=[id]))
    tagged_entries = relationship("KeywordAssociation", back_populates='keyword')

    def path(self):
        """Keyword path

        Returns the full keyword path for the given level. 
        The levels are separated by a '>' sign. The levels are:

        Topic > Term > Variable_Level_1 > Variable_Level_2 > Detailed_Variable

        Returns
        -------

        path : str
            The full keyword path
        """
        path = [self.value]
        parent = self.parent

        while parent is not None:
            path.append(parent.value)
            parent = self.parent
        
        return ' > '.join(reversed(path))

    def as_dict(self):
        return {
            'path': self.path,
            'alias': self.alias,
            'value': self.value,
            'uuid': self.uuid
        }

    def __str__(self):
        return self.path()


class KeywordAssociation(Base):
    __tablename__ = 'nm_keywords_entries'

    # columns
    keyword_id = Column(Integer, ForeignKey('keywords.id'), primary_key=True)
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    alias = Column(String(1024))
    associated_value = Column(String(1024))

    # relationships
    keyword = relationship("Keyword", back_populates='tagged_entries')
    entry = relationship("Entry", back_populates='keywords')

    def __str__(self):
        return "<Entry ID=%d> tagged %s" % (self.entry.id, self.keyword.value)
