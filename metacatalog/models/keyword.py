from sqlalchemy import Column, ForeignKey, event
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, backref, object_session


from metacatalog.db import Base


class Keyword(Base):
    __tablename__ = 'keywords'

    # columns
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('keywords.id'))
    value = Column(String(1024))
    uuid = Column(String(64), unique=True)
    full_path = Column(String)

    # relationships
    children = relationship("Keyword", backref=backref('parent', remote_side=[id]))
    tagged_entries = relationship("KeywordAssociation", back_populates='keyword')

    def path(self):
        """Keyword path

        Returns the full keyword path for the given level. 
        The levels are separated by a '>' sign. The levels are:

        Topic > Term > Variable_Level_1 > Variable_Level_2 > Variable_Level_3 > Detailed_Variable

        Returns
        -------

        path : str
            The full keyword path
        """
        path = [self.value]
        parent = self.parent

        while parent is not None:
            path.append(parent.value)
            parent = parent.parent
        
        return ' > '.join(reversed(path))

    def as_dict(self):
        return {
            'path': self.path,
            'alias': self.alias,
            'value': self.value,
            'uuid': self.uuid
        }

    def __str__(self):
        return "%s <ID=%d>" % (self.path(), self.id)


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


@event.listens_for(Keyword, 'after_insert')
def insert_full_path(mapper, connection, keyword):
    """
    Create the full path to the current entry in Python and
    save it as a string back into the database.
    """
    if keyword.full_path is None:
        keyword.full_path = keyword.path()
    
    # commit changes
    session = object_session(keyword)
    session.add(keyword)
    session.commit()