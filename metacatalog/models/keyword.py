from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, backref


from metacatalog.db.base import Base


class Keyword(Base):
    __tablename__ = 'keywords'

    # columns
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('keywords.id'))
    value = Column(String(1024), nullable=False)
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
            'path': self.path(),
            'alias': self.alias,
            'value': self.value,
            'uuid': self.uuid
        }

    def __str__(self):
        return "%s <ID=%d>" % (self.path, self.id)


class KeywordAssociation(Base):
    r"""Keyword association

    Association between keyword and entry.

    Attributes
    ----------
    keyword_id : int
        Unique ID of the `metacatalog.models.Keyword`
    entry_id : int
        Unique ID of the `metacatalog.models.Entry`
    alias : str
        Alias for the Keyword in the context of this
        Entry
    associated_value : str
        A Entry specific value associated to this
        relation

        .. deprecated:: 0.1.6
            `associated_value` will be removed in 0.2. Rather use 
            `metacatalog.models.Detail` to store custom key-values 
    
    """
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

