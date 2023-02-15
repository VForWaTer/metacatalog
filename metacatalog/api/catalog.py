"""CATALOG API

The catalog API offers application wide endpoints that are not bound to a
specific API action or model

"""
from __future__ import annotations
from typing import overload, TYPE_CHECKING
if TYPE_CHECKING:
        from sqlalchemy.orm import Session
        from metacatalog.models import Entry, EntryGroup, Person, Keyword
from typing_extensions import Literal
from sqlalchemy.orm.exc import NoResultFound

from metacatalog import api
from metacatalog.util.logging import get_logger
from metacatalog.util.results import ImmutableResultSet
from metacatalog.models import Entry, EntryGroup, Keyword, Person

@overload
def get_uuid(as_result: Literal[False] = ...) -> 'Entry' | 'EntryGroup' | 'Person' | 'Keyword' | None: ...
@overload
def get_uuid(as_result: Literal[True] = ...) -> ImmutableResultSet: ...
def get_uuid(session: 'Session', uuid: str, as_result: bool = False, not_found: str = 'raise') -> ImmutableResultSet | 'Entry' | 'EntryGroup' | 'Person' | 'Keyword' | None:
    """
    Return the Metacatalog object of given
    version 4 UUID. The supported objects are:

    - Entry
    - EntryGroup
    - Keyword
    - Person

    .. versionadded:: 0.1.13

    .. versionchanged:: 0.2.7

        Now, also :class:`Persons <metacatalog.models.Person>` can be
        found by UUID
    
    .. versionchanged:: 0.7.5
        Found Entry and EntryGroup can be returned as ImmutableResultSet
        now.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    uuid : str
        Find by version 4 UUID.
    as_result : bool
        .. versionadded:: 0.7.5
        
        If True, the found Entry or Entrygroup will be merged into a
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>`.
        Ignored for matched Keyword and Person.
        Defaults to False.

    Returns
    -------
    record : Entry, EntryGroup, Keyword, Person, ImmutableResultSet, None
        Matched Entry, EntryGroup, Keyword, Person or ImmutableResultSet.
        If no match was found, None is returned.

    """
    # check if an Entry exists
    entry = api.find_entry(session, uuid=uuid)
    if entry is not None:
        if as_result:
            return ImmutableResultSet(entry)
        else:
            return entry

    # check if Entrygroup exists
    group = api.find_group(session, uuid=uuid)
    if group is not None:
        if as_result:
            return ImmutableResultSet(group)
        else:
            return group

    # check if a Person exists
    person = api.find_person(session, uuid=uuid)
    if person is not None:
        return person

    # handle keyword
    keyword = api.find_keyword(session, uuid=uuid)
    if keyword is not None:
        return keyword

    if not_found == 'raise':
        raise NoResultFound("The UUID='%s' was not found." % uuid)
    else:
        return None
