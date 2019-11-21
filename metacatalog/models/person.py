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
