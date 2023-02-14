"""CATALOG API

The catalog API offers application wide endpoints that are not bound to a
specific API action or model

"""
from typing import Union, TYPE_CHECKING
if TYPE_CHECKING:
        from sqlalchemy.orm import Session
        from metacatalog.models import Entry, EntryGroup, Person, Keyword

from sqlalchemy.orm.exc import NoResultFound

from metacatalog import api
from metacatalog.util.logging import get_logger


def get_uuid(session: 'Session', uuid: str, not_found: str = 'raise') -> Union['Entry', 'EntryGroup', 'Person', 'Keyword']:
    """
    Return the Metacatalog object of given
    version 4 UUID. The supported objects are:

    - Entry
    - EntryGroup
    - Keyword
    - Person

    .. versionadded:: 0.1.13

    .. versionchanged:: 0.2.7

        Now, also :class:`Persons <metacatalog.model.Person>` can be
        found by UUID

    """
    # check if an Entry exists
    entry = api.find_entry(session, uuid=uuid)
    if entry is not None:
        return entry

    # check if Entrygroup exists
    group = api.find_group(session, uuid=uuid)
    if group is not None:
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
