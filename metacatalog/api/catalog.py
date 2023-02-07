"""CATALOG API

The catalog API offers application wide endpoints that are not bound to a
specific API action or model

"""
import os
import glob


from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound


from metacatalog import api
from metacatalog.util.results import ImmutableResultSet
from metacatalog.util.logging import get_logger


def get_uuid(session: Session, uuid: str, not_found='raise'):
    """
    Return the Metacatalog object of given
    version 4 UUID. The supported objects are:

    - Entry
    - EntryGroup
    - Keyword
    - Person

    .. versionadded:: 0.1.13

    .. versionchanged:: 0.2.7
        Now, also :class:`Persons <metacatalog.model.Person` can be
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


def regenerate_iso19115(session: Session, config_dict: dict, path: str) -> None:
    """
    Generate ISO 19115 XML files for all ImmutableResultSets in the
    database session. The XML files are saved in the folder given in
    ``path``, existing files in the folder are deleted, so use this 
    function with caution.

    .. versionadded:: 0.7.4

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    config_dict : dict
        Configuration dictionary, containing information about the data provider
    path : str
        If given, a file location for export. 

    """
    irs_checksums = []

    # use absolute path
    path = os.path.abspath(path)

    # remove everything in path
    for file in glob.glob(f"{path}/*"):
        os.remove(file)

    for entry in api.find_entry(session):
        # get the checksum of the ImmutableResultSet
        irs_checksum = ImmutableResultSet(entry).checksum

        # if irs_checksum in irs_checksums: ImmutableResultSet already exported -> continue
        if irs_checksum in irs_checksums:
            continue
        else:
            entry.export_iso19115(config_dict, path=f"{path}/iso19115_{irs_checksum}.xml")

            irs_checksums.append(irs_checksum)
