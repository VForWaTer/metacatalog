from typing import TYPE_CHECKING, List
if TYPE_CHECKING:
    from metacatalog.models import Entry

from uuid import uuid4

from sqlalchemy import Column, ForeignKey, CheckConstraint
from sqlalchemy import Integer, String, Boolean
from sqlalchemy.orm import relationship, Session

from metacatalog.db.base import Base
from metacatalog.util.exceptions import MetadataMissingError


class Person(Base):
    """
    Person represents all persons that are associated
    in any kind to a dataset. This may be an author, who is
    mandatory, but also an owners or processors can be
    associated.

    .. note::

        In metatacatalog, an organisation_name is an optional, but
        recommended information. On export to ISO 19115 persons without
        affilated organisations can't be exported. Thus, they should
        not take the role of ``pointOfContact`` or ``author`` (first author),
        because this would result in invalid ISO 19115 metadata then.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    uuid : str
        .. versionadded: 0.2.7

        Version 4 UUID string to identify the Entry across installations.
        This field is read-only and will be assigned on creation. It is primarily
        used to export Entry into ISO19115 metadata.
    first_name : str
        .. versionchanged:: 0.1.10
            Now mandatory.
        
        Person's first name.
    last_name : str
        Person's last name.
    organisation_name : str
        Optional, but **highly recommended** if applicable. This should be
        the name of the head organisation, **without department information**.
        The full :attr:`affiliation` can be defined in another attribute.
    affiliation : str
        Optional. The user may want to further specify a department or
        working group for affiliation information.
    attribution : str
        .. versionadded: 0.1.10

        Optional. The user may define an attribtion recommondation here,
        which is associated to all datasets, the user is first author of.
        If not given, the system running metacatalog should give automatic
        and fallback information of how a dataset should be attributed.
    entries : list
        List of :class:`Entries <metacatalog.models.Entry>` the user is
        associated to. This includes all kinds of associations, not only
        author and coAuthor associations.

    """
    __tablename__ = 'persons'
    __table_args__ = (
        CheckConstraint('NOT (last_name is NULL AND organisation_name is NULL)'),
    )

    # columns
    id = Column(Integer, primary_key=True)
    uuid = Column(String(36), nullable=False, default=lambda: str(uuid4()))
    is_organisation = Column(Boolean, default=False)
    first_name = Column(String(128), nullable=True)
    last_name = Column(String(128), nullable=True)
    organisation_name = Column(String(1024), nullable=True)
    organisation_abbrev = Column(String(64), nullable=True)
    affiliation = Column(String(1024))
    attribution = Column(String(1024))

    # relationships
    entries: List['Entry'] = relationship("PersonAssociation", back_populates='person')

    def to_dict(self, deep: bool = False) -> dict:
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
        if self.is_organisation:
            d = dict(
                id=self.id,
                uuid=self.uuid
            )
        else:
            d = dict(
                id=self.id,
                uuid=self.uuid,
                first_name=self.first_name,
                last_name=self.last_name
            )

        # set optionals
        for attr in ('organisation_abbrev', 'organisation_name', 'affiliation', 'attribution'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # deep loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    @classmethod
    def from_dict(cls, data: dict, session: Session) -> 'Person':
        """
        Create a new Person from a python dictionary.

        Parameters
        ----------
        data : dict
            The dictionary containing the data
        session : Session
            The database session

        Returns
        -------
        obj : Person
            The new Person

        """
        # check if there is an ID in the data
        if 'id' in data:
            from metacatalog import api
            try:
                return api.find_person(session, id=data['id'], return_iterator=True).one()
            except:
                raise MetadataMissingError(f"Person with id {data['id']} not found in database.")                
        
        # create
        person = cls(**data)

        # add the person
        try:
            session.add(person)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        
        return person

    @property
    def full_name(self) -> str:
        if not self.is_organisation:
            if self.first_name is not None:
                return '%s %s' % (self.first_name, self.last_name)
            else:
                return self.last_name
        else:
            return '%s (Org.)' % self.organisation_name

    @full_name.setter
    def full_name(self, name: str):
        if not self.is_organisation:
            # split the name and use the last name
            chunks = name.split(' ')
            self.last_name = chunks.pop()

            if len(chunks) > 0:
                self.first_name = ' '.join(chunks)
            else:
                self.first_name = None
        else:
            part = name.replace(' (Org.)', '')
            self.organisation_name = part

    def __str__(self) -> str:
        return "%s <ID=%d>" % (self.full_name, self.id)


class PersonRole(Base):
    __tablename__ = 'person_roles'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    description = Column(String, nullable=True)

    # relationships
    persons_with_role: List['PersonAssociation'] = relationship("PersonAssociation", back_populates='role')

    def to_dict(self, deep: bool = False) -> dict:
        """
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

    def __str__(self) -> str:
        return "%s <ID=%d>" % (self.name, self.id)


class PersonAssociation(Base):
    __tablename__ = 'nm_persons_entries'

    # columns
    person_id = Column(Integer, ForeignKey('persons.id'), primary_key=True)
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    relationship_type_id = Column(Integer, ForeignKey('person_roles.id'), nullable=False)
    order = Column(Integer, nullable=False)

    # relationships
    role: PersonRole = relationship("PersonRole", back_populates='persons_with_role')
    person: Person = relationship("Person", back_populates='entries')
    entry: 'Entry' = relationship("Entry", back_populates='contributors')

    def to_dict(self, deep: bool = False) -> dict:
        """
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

    def __str__(self) -> str:
        return '%s <ID=%d> as %s for Entry <ID=%d>' % (self.person.full_name, self.person.id, self.role.name, self.entry.id)
