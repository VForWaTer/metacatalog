from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship

from metacatalog.db.base import Base


class EntryGroupAssociation(Base):
    __tablename__ = 'nm_entrygroups'

    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    group_id = Column(Integer, ForeignKey('entrygroups.id'), primary_key=True)


class EntryGroupType(Base):
    __tablename__ = 'entrygroup_types'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(40), nullable=False)
    description = Column(String, nullable=False)

    # relationships
    entries = relationship("EntryGroup", back_populates='type')

    def to_dict(self, deep=False) -> dict:
        """To dict

        Return the model as a python dictionary.

        Parameters
        ----------
        deep : bool
            If True, all related objects will be included as 
            dictionary. Defaults to False

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            id=self.id,
            name=self.name,
            description=self.description
        )

        # lazy loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    def __str__(self):
        return "%s <ID=%d>" % (self.name, self.id)


class EntryGroup(Base):
    __tablename__ = 'entrygroups'

    # columns
    id = Column(Integer, primary_key=True)
    type_id = Column(Integer, ForeignKey('entrygroup_types.id'), nullable=False)
    title = Column(String(40))
    description = Column(String)

    # relationships
    type = relationship("EntryGroupType", back_populates='entries')
    entries = relationship("Entry", secondary="nm_entrygroups", back_populates="associated_groups")

    def to_dict(self, deep=False) -> dict:
        """To dict

        Return the model as a python dictionary.

        Parameters
        ----------
        deep : bool
            If True, all related objects will be included as 
            dictionary. Defaults to False

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            id=self.id,
            type=self.type.to_dict(deep=False)
        )

        # set optionals
        for attr in ('title', 'description'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # lazy loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    def __str__(self):
        return "%s%s <ID=%d>" % (
            self.type.name, 
            " %s" % self.title[:20] if self.title is not None else '', 
            self.id
        )
