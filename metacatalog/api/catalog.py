"""CATALOG API

The catalog API offers application wide endpoints that are not bound to a
specific API action or model

"""
from typing import Union
import os
import glob

from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from metacatalog import api
from metacatalog.util.logging import get_logger
from metacatalog.util.results import ImmutableResultSet
from metacatalog.models import Entry, EntryGroup, Keyword, Person

def get_uuid(session: Session, uuid: str, as_result: bool=False, not_found: str='raise') -> Union[Entry, EntryGroup, Keyword, Person, ImmutableResultSet, None]:
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


def create_iso19115(session: Session, config_dict: dict, path: str, if_exists: str = 'fail', verbose: bool = False) -> None:
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
        Folder location where all ISO19115 XML files are saved to.
    if_exists: {'fail', 'replace'}, default 'fail'
        How to behave if the XML file for the ImmutableResultSet already exists in path.

        * fail: Raise a ValueError
        * replace: Overwrite the existing XML file.
    verbose: bool, default False
        Enable verbose output.        

    """
    from metacatalog.ext.standard_export.util import _get_uuid
    from tqdm import tqdm

    if if_exists not in ("fail", "replace"):
        raise ValueError(f"'{if_exists}' is not valid for if_exists")

    irs_uuids = []

    # use absolute path
    path = os.path.abspath(path)

    # list files to check if a file already exists
    files = os.listdir(path)

    # create the generator 
    if verbose:
        gen = tqdm(api.find_entry(session))
    else:
        gen = api.find_entry(session)

    for entry in gen:
        # get the uuid of the ImmutableResultSet that is written to ISO19115 XML (rs.group.uuid or rs.get('uuid'))
        irs_uuid = _get_uuid(ImmutableResultSet(entry))

        # if irs_uuid in irs_uuids: ImmutableResultSet already exported -> continue
        if irs_uuid in irs_uuids:
            continue
        else:
            # check if_exists policy first
            if any(irs_uuid in file for file in files):
                if if_exists == 'fail':
                    raise ValueError(f"ISO19115 XML file for uuid '{irs_uuid}' already exists under {path}.")
            
            entry.export_iso19115(config_dict, path=f"{path}/iso19115_{irs_uuid}.xml")

            irs_uuids.append(irs_uuid)
