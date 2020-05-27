from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship

from metacatalog.db.base import Base


class Person(Base):
    __tablename__ = 'persons'

    id = Column(Integer, primary_key=True)
    first_name = Column(String(128), nullable=True)
    last_name = Column(String(128), nullable=False)
    affiliation = Column(String(1024))

    # relationships
    entries = relationship("PersonAssociation", back_populates='person')

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
            last_name=self.last_name
        )

        # set optionals
        for attr in ('first_name', 'affiliation'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # deep loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    @property
    def full_name(self):
        if self.first_name is not None:
            return '%s %s' % (self.first_name, self.last_name)
        else:
            return self.last_name

    @full_name.setter
    def full_name(self, name):
        # split the name and use the last name
        chunks = name.split(' ')
        self.last_name = chunks.pop()

        if len(chunks) > 0:
            self.first_name = ' '.join(chunks)
        else:
            self.first_name = None

    def __str__(self):
        return "%s <ID=%d>" % (self.full_name, self.id)


class PersonRole(Base):
    __tablename__ = 'person_roles'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    description = Column(String, nullable=True)

    # relationships
    persons_with_role = relationship("PersonAssociation", back_populates='role')

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
        )

        # set optionals
        for attr in ('description'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # deep loading
        if deep:
            d['persons_with_role'] = [p.to_dict() for p in self.persons_with_role]

        return d

    def __str__(self):
        return "%s <ID=%d>" % (self.name, self.id)


class PersonAssociation(Base):
    __tablename__ = 'nm_persons_entries'

    # columns 
    person_id = Column(Integer, ForeignKey('persons.id'), primary_key=True)
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    relationship_type_id = Column(Integer, ForeignKey('person_roles.id'), nullable=False)
    order = Column(Integer, nullable=False)

    # relationships
    role = relationship("PersonRole", back_populates='persons_with_role')
    person = relationship("Person", back_populates='entries')
    entry = relationship("Entry", back_populates='contributors')

    def to_dict(self, deep=False) -> dict:
        """To dict

        Return the model as a python dictionary.

        Parameters
        ----------
        deep : bool
            Parameter will be ignored, but is defined to use the same method
            interface as all other models

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            person=self.person.to_dict(deep=False),
            entry=self.entry.to_dict(deep=False),
            role=self.role.to_dict(deep=False),
            order=self.order
        )

        return d

    def __str__(self):
        return '%s <ID=%d> as %s for Entry <ID=%d>' % (self.person.full_name, self.person.id, self.role.name, self.entry.id)
