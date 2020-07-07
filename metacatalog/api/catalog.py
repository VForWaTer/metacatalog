"""CATALOG API

The catalog API offers application wide endpoints that are not bound to a 
specific API action or model

"""
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from metacatalog import api

def get_uuid(session: Session, uuid: str, not_found='raise'):
    """
    .. versionadded:: 0.1.13
    
    Return the Metacatalog object of given 
    version 4 UUID. The supported objects are:

    - Entry
    - EntryGroup
    - Keyword

    """
    # check if a Entry exists
    entry = api.find_entry(session, uuid=uuid)
    if entry is not None:
        return entry

    # check if Entrygroup exists
    group = api.find_group(session, uuid=uuid)
    if group is not None:
        return group

    # handle keyword
    keyword = api.find_keyword(session, uuid=uuid)
    if keyword is not None:
        return keyword
    
    if not_found == 'raise':
        raise NoResultFound("The UUID='%s' was not found." % uuid)
    else:
        return None
