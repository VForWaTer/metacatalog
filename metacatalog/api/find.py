"""FIND operation

A find operation returns objects from the metacatalog on exact matches.
At the current stage, the following objects can be found by a FIND operation:

* keywords

"""
from __future__ import annotations
from typing import List, Union, overload, Optional, TYPE_CHECKING
from typing_extensions import Literal
if TYPE_CHECKING:
        from sqlalchemy.orm import Session, Query
        from metacatalog.db.base import Base
        from metacatalog.models import Keyword, Thesaurus, License, Unit, Variable, DataSourceType, PersonRole, Person, EntryGroupType, EntryGroup, Entry
import os
import warnings

import numpy as np
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import false, true

from metacatalog import models
from metacatalog.util import location
from metacatalog.util.results import ImmutableResultSet

import nltk


def _match(column_instance: InstrumentedAttribute, compare_value: str, invert: bool = False) -> BinaryExpression:
    """
    For building filters, the Column should be filtered for
    records that match the given value. If the compare value
    contains a `'%'` or `'*'`, a LIKE clause instrumenting this
    wildcard will be used, otherwise an exact match.
    If the string startswith `!`, the filter will be inverted.

    Parameters
    ----------
    column_instance : sqlalchemy.Column
        The column that the filter should be build upon
    compare_value : str
        Matching string that should be used.
    invert : bool
        If True, a unary `not` will be placed on the comparison.
        This is not actually used in the models. Defaults to False.

    Returns
    -------
    expression : sqlalchemy.BinaryExpression
        The returned BinaryExpression can directly be passed to
        sqlalchemy's filter function on Query objects.

    TODO
    ----
    here, we could check the content for `[]` and apply regex.

    """
    # check invert
    if compare_value.startswith('!'):
        invert = True
        compare_value = compare_value[1:]

    # check for asterisk
    if '*' in compare_value:
        compare_value = compare_value.replace('*', '%')

    # check for the right variant
    if '%' in compare_value:
        if invert:
            return column_instance.notlike(compare_value)
        else:
            return column_instance.like(compare_value)
    else:
        if invert:
            return column_instance!=compare_value
        else:
            return column_instance==compare_value


@overload
def find_keyword(return_iterator: Literal[False] = False) -> List['Keyword']: ...
@overload
def find_keyword(return_iterator: Literal[True] = False) -> 'Query': ...
def find_keyword(session: 'Session', id: Optional[int] = None, uuid: Optional[str] = None, value: Optional[str] = None, full_path: Optional[str] = None, thesaurus_name: Optional[str] = None, return_iterator: bool = False) -> List['Keyword'] | 'Query':
    """
    Return one or many keyword entries from the database on
    exact matches.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    uuid : str
        .. versionadded:: 0.1.13

        Find by version 4 UUID. If uuid is given, all other options
        will be ignored.
    value : str
        Value of the requested keyword(s). Multiple record
        return is possible.
    full_path : str
        .. versionadded:: 0.8.4

        Full path of the requested keyword.
    thesaurus_name : str
        .. versionadded:: 0.1.10

        The name of the thesaurus, the keyword originates from.
        At the current stage, only 'GCMD' science keywords are
        implemented.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.Keyword
        List of matched Keyword instance.

    """
    # base query
    query = session.query(models.Keyword)

    # handle uuid first
    if uuid is not None:
        query = query.filter(models.Keyword.uuid==uuid)
        if return_iterator:
            return query
        else:
            return query.first()

    # add needed filter
    if id is not None:
        query = query.filter(models.Keyword.id==id)
    if full_path is not None:
        query = query.filter(_match(models.Keyword.full_path, full_path))
    if value is not None:
        query = query.filter(_match(models.Keyword.value, value))
    if thesaurus_name is not None:
        query = query.filer(_match(models.Keyword.thesaurusName.name, thesaurus_name))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_thesaurus(return_iterator: Literal[False] = False) -> List['Thesaurus']: ...
@overload
def find_thesaurus(return_iterator: Literal[True] = False) -> 'Query': ...
def find_thesaurus(session: 'Session', id: Optional[int] = None, uuid: Optional[str] = None, name: Optional[str] = None, title: Optional[str] = None, organisation: Optional[str] = None, description: Optional[str] = None, return_iterator: bool = False) -> 'Query' | List['Thesaurus']:
    """
    Retun one or many thesaurii references from the database
    on exact matches. You can  use `'%'` and `'*'` as wildcards
    and prepend a str with `!` to invert the filter.

    ..versionadded:: 0.1.10

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    uuid : str
        .. versionadded:: 0.6.6

        Find by version 4 UUID. If uuid is given, all other options
        will be ignored.
    name : str
        Short name of the Thesaurus. No wildcard use is possible.
        Names are unique, thus no multiple thesaurii will be found.
    title : str
        Full title attribute of the requested thesaurii.
        Multiple record return is possible.
    organisation : str
        Organisation name of the requested thesaurii.
        Multiple record return is possible.
    description : str
        Description of the thesaurus. The decription field
        is optional and some records may not be found.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.models.Thesaurus
        List of matched Thesaurus instances.

    """
    # base query
    query = session.query(models.Thesaurus)

    # handle uuid first
    if uuid is not None:
        query = query.filter(models.Thesaurus.uuid==uuid)
        if return_iterator:
            return query
        else:
            return query.first()

    # add needed filter
    if id is not None:
        query = query.filter(models.Thesaurus.id==id)
    if name is not None:
        query = query.filter(models.Thesaurus.name==name)
    if title is not None:
        query = query.filter(_match(models.Thesaurus.title, title))
    if organisation is not None:
        query = query.filter(_match(models.Thesaurus.organisation, organisation))
    if description is not None:
        query = query.filer(_match(models.Thesaurus.description, description))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_license(return_iterator: Literal[False] = False) -> List['License']: ...
@overload
def find_license(return_iterator: Literal[True] = False) -> 'Query': ...
def find_license(session: 'Session', id: Optional[int] = None, title: Optional[str] = None, short_title: Optional[str] = None, by_attribution: bool = None, share_alike: bool = None, commercial_use: bool = None, return_iterator: bool = False) -> 'Query' | List['License']:
    """
    Return one or many license entries from the database on
    exact matches. You can  use `'%'` and `'*'` as wildcards
    and prepend a str with `!` to invert the filter.

    .. versionchanged:: 0.1.8
        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    title : str
        .. versionadded:: 0.1.8

        Full title attribute of the requested license(s).
        Multiple record return is possible.
    short_title : str
        short_title attribute of the requested license(s).
        Multiple record return is possible.
    by_attribution : bool
        by_attribution attribute of the requested license(s).
        Multiple record return is possible.
    share_alike : bool
        by_attribution attribute of the requested license(s).
        Multiple record return is possible.
    commercial_use : bool
        by_attribution attribute of the requested license(s).
        Multiple record return is possible.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.License
        List of matched License instance.
    """
    # base query
    query = session.query(models.License)

    # add needed filter
    if id is not None:
        query = query.filter(models.License.id==id)
    if title is not None:
        query = query.filter(_match(models.License.title, title))
    if short_title is not None:
        query = query.filter(_match(models.License.short_title, short_title))
    if by_attribution is not None:
        query = query.filter(models.License.by_attribution==by_attribution)
    if share_alike is not None:
        query = query.filter(models.License.share_alike==share_alike)
    if commercial_use is not None:
        query = query.filter(models.License.commercial_use==commercial_use)

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_unit(return_iterator: Literal[False] = False) -> List['Unit']: ...
@overload
def find_unit(return_iterator: Literal[True] = False) -> 'Query': ...
def find_unit(session: 'Session', id: Optional[int] = None, name: Optional[str] = None, symbol: Optional[str] = None, return_iterator: bool = False) -> 'Query' | List['Unit']:
    """
    Return one unit entry from the database on
    exact matches. It makes only sense to set one of the
    attributes (id, name, symbol).

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    name : str
        name attribute of the requested unit.
    symbol : str
        symbol attribute of the requested unit.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.Unit
        List of matched Unit instance.

    """
    # base query
    query = session.query(models.Unit)

    if id is not None:
        query = query.filter(models.Unit.id==id)
    if name is not None:
        query = query.filter(_match(models.Unit.name, name))
    if symbol is not None:
        query = query.filter(_match(models.Unit.symbol, symbol))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_variable(return_iterator: Literal[False] = False) -> List['Variable']: ...
@overload
def find_variable(return_iterator: Literal[True] = False) -> 'Query': ...
def find_variable(session: 'Session', id: Optional[int] = None, name: Optional[str] = None, symbol: Optional[str] = None, return_iterator: bool = False) -> 'Query' | List['Variable']:
    """
    Return one vriable entry from the database on
    exact matches. It makes only sense to set one of the
    attributes (id, name, symbol).

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    name : str
        name attribute of the requested variable.
    symbol : str
        symbol attribute of the requested variable.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.Variable
        List of matched Variable instance.

    """
    # base query
    query = session.query(models.Variable)

    if id is not None:
        query = query.filter(models.Variable.id==id)
    if name is not None:
        query = query.filter(_match(models.Variable.name, name))
    if symbol is not None:
        query = query.filter(_match(models.Variable.symbol, symbol))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_datasource_type(return_iterator: Literal[False] = False) -> List['DataSourceType']: ...
@overload
def find_datasource_tyoe(return_iterator: Literal[True] = False) -> 'Query': ...
def find_datasource_type(session: 'Session', id: Optional[int] = None, name: Optional[str] = None, return_iterator: bool = False) -> 'Query' | List['DataSourceType']:
    """
    Return one datasource type record on exact matches.
    Types can be identified by id or name.

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    name : str
        name attribute of the requested type.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.DataSourceType
        List of matched DataSourceType instance.

    """
    # base query
    query = session.query(models.DataSourceType)

    if id is not None:
        query = query.filter(models.DataSourceType.id==id)
    if name is not None:
        query = query.filter(_match(models.DataSourceType.name, name))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_role(return_iterator: Literal[False] = False) -> List['PersonRole']: ...
@overload
def find_role(return_iterator: Literal[True] = False) -> 'Query': ...
def find_role(session: 'Session', id: Optional[int] = None, name: Optional[str] = None, return_iterator: bool = False) -> 'Query' | List['PersonRole']:
    """
    Return one person role record on exact matches.
    Roles can be identified by id or name.

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    name : str
        name attribute of the requested role.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.PersonRole
        List of matched PersonRole instance.

    """
    # base query
    query = session.query(models.PersonRole)

    if id is not None:
        query = query.filter(models.PersonRole.id==id)
    if name is not None:
        query = query.filter(_match(models.PersonRole.name, name))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_person(return_iterator: Literal[False] = False) -> List['Person']: ...
@overload
def find_person(return_iterator: Literal[True] = False) -> 'Query': ...
def find_person(
    session: 'Session',
    id: Optional[int] = None,
    uuid: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    role: Optional[int | str] = None, 
    organisation_name: Optional[str] = None,
    organisation_abbrev: Optional[str] = None,
    attribution: Optional[str] = None, 
    return_iterator: bool = False
) -> 'Query' | List['Person']:
    """
    Return person record on exact matches. Persons can be
    identified by id, first_name, last_name, organisation details or associated roles.
    Since version ``0.2.5`` only Persons which have a ``is_organisation==False``
    will be returned

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    .. versionchanged:: 0.2.6

        organisation_abbrev is now available.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    uuid : str
        .. versionadded:: 0.2.7

        Find by version 4 UUID. If uuid is given, all other options
        will be ignored.
    first_name : str
        First name attribute of the requested person.
    last_name : str
        Last name attribute of the requested person.
    role : int, str
        Role id or name (exact match) that is associated to
        a person. Will most likely return many persons.
    organisation_name :  str
        .. versionadded:: 0.1.10

        The name of the head organisation, without department
        and group specification.
        
        .. note::

            Not all Persons may have an organisation_name.

    organisation_abbrev : str
        .. versionadded:: 0.2.6

        A short abbreviation of the head organisation if
        applicable.
        
        .. note::

            Not all Persons may have a head organisation

    attribution : str
        .. versionadded:: 0.2.8

        Attribtion recommondation, which is associated 
        to all datasets, the user is first author of
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.Person
        List of matched Person instance.

    See Also
    --------
    find_organisation

    """
    # base query
    query = session.query(models.Person).filter(models.Person.is_organisation == false())

    # handle uuid first
    if uuid is not None:
        query = query.filter(models.Person.uuid==uuid)
        if return_iterator:
            return query
        else:
            return query.first()

    if id is not None:
        query = query.filter(models.Person.id==id)

    if first_name is not None:
        query = query.filter(_match(models.Person.first_name, first_name))

    if last_name is not None:
        query = query.filter(_match(models.Person.last_name, last_name))

    if role is not None:
        # get the roles
        if isinstance(role, int):
            role_id = session.query(models.PersonRole.id).filter(models.PersonRole.id==role).one()
        elif isinstance(role, str):
            role_id = session.query(models.PersonRole.id).filter(models.PersonRole.name==role).first()
        else:
            raise AttributeError('Role has to be an id (integer) or name (string).')

        # find the associations
        ids = session.query(models.PersonAssociation.person_id).filter(models.PersonAssociation.relationship_type_id==role_id).all()

        # filter by these ids
        query = query.filter(models.Person.id.in_(ids))

    if organisation_name is not None:
        query = query.filter(_match(models.Person.organisation_name, organisation_name))

    if organisation_abbrev is not None:
        query = query.filter(_match(models.Person.organisation_abbrev, organisation_abbrev))

    if attribution is not None:
        query = query.filter(_match(models.Person.attribution, attribution))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_organisation(return_iterator: Literal[False] = False) -> List['Person']: ...
@overload
def find_organisation(return_iterator: Literal[True] = False) -> 'Query': ...
def find_organisation(session: 'Session', id: Optional[int] = None, organisation_name: Optional[str] = None, organisation_abbrev: Optional[str] = None, role: Union[int, str] = None,  return_iterator: bool = False) -> 'Query' | List['Person']:
    """
    .. versionadded:: 0.2.6

    Return Person record on exact matches. This function will only return records
    that have ``is_organisation=True``. For natural persons use the
    :func:`find_person <metacatalog.api.find_person>` function.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    organisation_name :  str
        Required. The full name of the head organisation.
    organisation_abbrev : str
        A short abbreviation of the head organisation if
        applicable.
    role : int, str
        Role id or name (exact match) that is associated to
        an organistion. Will most likely return many organisations.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.Person
        List of matched Person instance.

    """
    # base query
    query = session.query(models.Person).filter(models.Person.is_organisation == true())

    if id is not None:
        query = query.filter(models.Person.id == id)

    if organisation_name is not None:
        query = query.filter(_match(models.Person.organisation_name, organisation_name))

    if organisation_abbrev is not None:
        query = query.filter(_match(models.Person.organisation_abbrev, organisation_abbrev))

    if role is not None:
        # get the roles
        if isinstance(role, int):
            role_id = session.query(models.PersonRole.id).filter(models.PersonRole.id == role).one()
        elif isinstance(role, str):
            role_id = session.query(models.PersonRole.id).filter(models.PersonRole.name == role).first()
        else:
            raise AttributeError(
                'Role has to be an id (integer) or name (string).')

        # find the associations
        ids = session.query(models.PersonAssociation.person_id).filter(
            models.PersonAssociation.relationship_type_id == role_id).all()

        # filter by these ids
        query = query.filter(models.Person.id.in_(ids))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_group_type(return_iterator: Literal[False] = False) -> List['EntryGroupType']: ...
@overload
def find_group_type(return_iterator: Literal[True] = False) -> 'Query': ...
def find_group_type(session: 'Session', id: Optional[int] = None, uuid: Optional[str] = None, name: Optional[str] = None, return_iterator: bool = False) -> 'Query' | List['EntryGroupType']:
    """
    Find a group type on exact matches. The group types
    describes a collection of entries.

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    name : str
        Name attribute of the group type.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list of metacatalog.EntryGroupType
        List of matched EntryGroupType instance.

    """
    # base query
    query = session.query(models.EntryGroupType)

    if id is not None:
        query = query.filter(models.EntryGroupType.id==id)
    if name is not None:
        query = query.filter(_match(models.EntryGroupType.name, name))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


@overload
def find_group(return_iterator: Literal[False] = False, as_result: Literal[False] = False) -> List['EntryGroup']: ...
@overload
def find_group(return_iterator: Literal[True] = False, as_result: bool = ...) -> 'Query': ...
@overload
def find_group(return_iterator: Literal[False] = False, as_result: Literal[True] = False) -> List['ImmutableResultSet']: ...

def find_group(session: 'Session', id: Optional[int] = None, uuid: Optional[str] = None, title: Optional[str] = None, type: Union[int, str] = None, return_iterator: bool = False, as_result: bool = False) -> 'Query' | List['EntryGroup'] | List['ImmutableResultSet']:
    """
    Find a group of entries on exact matches. Groups can be
    identified by id, title or its type.

    .. versionchanged:: 0.1.8

        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    .. versionchanged:: 0.2.14

        Can be returned as ImmutableResultSet now.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    uuid : str
        .. versionadded:: 0.1.13

        Find by version 4 UUID. If uuid is given, all other options
        will be ignored.
    title : str
        Title attribute of the group.
    type : int, str
        Either the id or name of a group type to exact match.
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.
    as_result : bool
        If True, the reuslts will be merged into a
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>`.
        Defaults to False. Will be ignored if return_iterator
        is True

    Returns
    -------
    records : list
        List of matched EntryGroup or ImmutableResultSet.

    """
    # base query
    query = session.query(models.EntryGroup)

    # handle uuid first
    if uuid is not None:
        query = query.filter(models.EntryGroup.uuid==uuid)
        if return_iterator:
            return query
        else:
            return query.first()

    # now the remaining parameters
    if id is not None:
        query = query.filter(models.EntryGroup.id==id)
    if title is not None:
        query = query.filter(_match(models.EntryGroup.title, title))
    if type is not None:
        if isinstance(type, int):
            grouptype = find_group_type(session=session, id=type, return_iterator=True).one()
        elif isinstance(type, str):
            grouptype = find_group_type(session=session, name=type, return_iterator=True).first()
        else:
            raise AttributeError('Type has to be an id (integer) or type name (string).')

        query = query.filter(models.EntryGroup.type_id==grouptype.id)

    # return
    if return_iterator:
        return query
    else:
        if as_result:
            return [ImmutableResultSet(group) for group in query.all()]
        return query.all()


@overload
def find_entry(return_iterator: Literal[False] = False, as_result: Literal[False] = False) -> List['Entry']: ...
@overload
def find_entry(return_iterator: Literal[True] = False, as_result: bool = ...) -> 'Query': ...
@overload
def find_entry(return_iterator: Literal[False] = False, as_result: Literal[True] = False) -> List['ImmutableResultSet']: ...

def find_entry(
    session: 'Session', 
    id: Optional[int] = None, 
    uuid: Optional[str] = None, 
    title: Optional[str] = None, 
    abstract: Optional[str] = None, 
    license: Optional[int | str] = None, 
    variable: Optional[int | str] = None, 
    external_id: Optional[str] = None, 
    version: Literal['latest'] | int ='latest', 
    project: Optional[int | str] = None, 
    author: Optional[int | str] = None,
    coauthor: Optional[int | str] = None,
    contributor: Optional[int | str]  =None,
    keywords: Optional[List[int | str]] = None, 
    details: Optional[List[int | str]] =None,
    include_partial: bool = False,
    return_iterator: bool = False,
    as_result: bool = False,
    by_geometry: Optional[str | List[float]] = None,
) -> 'Query' | List['Entry'] | List['ImmutableResultSet']:
    """
    Find an meta data Entry on exact matches. Entries can be
    identified by id, title, external_id and version. The
    version can be added to all other matching types, which
    are mutually exclusive.

    .. versionchanged:: 0.1.8

        string matches now allow ``'%'`` and ``'*'`` wildcards 
        and can be inverted by prepending ``!``
    
    .. versionchanged:: 0.2.14

        Can be returned as ImmutableResultSet now.

    .. versionchanged:: 0.3.9

        By setting include_partial to True, the API can now find
        partial Entries. This does only make sense in combination
        with ``as_result=True``, to lazy-load the complete record.

    .. versionchanged:: 0.7.4

        New arugment :attr:`coauthor` introduced, which is a functional
        replacement for the now deprecated 'contributor' argument.

    .. deprecated:: 0.7.4

        The contributor keyword is deprecated and will change its
        behavior in a future release. Use the new :attr:`coauthor`
        argument from now on. 

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will
        return only one record.
    uuid : str
        .. versionadded:: 0.1.13

        Find by version 4 UUID. If uuid is given, all other options
        will be ignored.
    title : str
        Title attribute of the Entry.
    abstract : str
        .. versionadded:: 0.1.8

        Abstract attibute of the Entry.

        .. note::

            The abstract is usually a full text and the FIND operation
            uses **exact** matches. Therefore be sure to use a wildcard

        .. code-block:: Python

            api.find_entry(session: 'Session', abstract='*phrase to find*')

    license : str, int
        .. versionadded:: 0.2.2

        The license can be a :class:`License <metacatalog.models.License>`,
        its id (int) or the short_title (str).
    variable : str, int
        .. versionadded:: 0.2.2

        The variable can be a :class:`Variable <metacatalog.models.Variable>`,
        its id (int) or the name (str).
    external_id : str
        External id attrinbute of the Entry.
    version : int, str
        .. versionchanged:: 0.2

            The default value is now 'latest'

        Version number of the Entry. Can be combined with
        the other matching parameters, as they might not be
        different between versions.
        If version == 'latest', only the latest version will be found.
        If None, all version are integrated.
    project : int, str
        .. versionadded:: 0.2.2
`
        The project can be a :class:`EntryGroup <metacatalog.models.EntryGroup>` of
        :class:`EntryGroupType.name=='Project' <metacatalog.models.EntryGroupType>`,
        its id (int) or title (str)
    author : int, str
        .. versionadded:: 0.2.2

        The author can be a :class:`Person <metacatalog.models.Person>`,
        his id (int) or name (str). A string argument will match first and last
        names. The author is only the first author. For other coauthor see
        :attr:`coauthor`.
    coauthor : int ,str
        .. versionadded:: 0.7.4

        The coauthor can be a :class:`Person <metacatalog.models.Person>`,
        his id (int) or name (str). A string argument will match first and last
        names. A  co author is anyone associated as first or co-author. For
        first author only, see :attr:`author`.
    contributor : int, str
        .. versionadded:: 0.2.2

        .. deprecated:: 0.7.4

            This argument will change its behavior with a future release.
            Use :attr:`coauthor` as a repalcement.

        The contributor can be a :class:`Person <metacatalog.models.Person>`,
        his id (int) or name (str). A string argument will match first and last
        names. A contributor is anyone associated as first or co-author. For
        first author only, see :attr:`author`.
    keywords : list of str, int
        .. versionadded:: 0.2.2

        The entries can be filtered by tagged controlled keywords. The given
        keyword or list of keywords will be matched against the value (str)  or
        id (int). If more than one is given, the entries need to be tagged by
        **all** keywords. An ``OR`` search is not possible, through the API.
    details : dict
        ..versionadded:: 0.2.2

        Entries can be filtered by additional details. The details need to be
        specified as dictioniares of ``name=value`` pairs. If more than one
        pair is given, the query will combine the pairs by ``AND``.
        An ``OR`` search is not possible, through the API.
        Search for value only, using a wildcard for the key ``*=value``.
    include_partial : bool
        .. versionadded:: 0.3.9

        Include partial entries into the response. Defaults to False.

        .. note::

            Partial Entries might not be usefull, as they can miss important
            metadata. Thus, it is highly recommended to set ``as_result=True``.
            Then, the returned 
            :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>`
            will lazy-load the sibling informations and merge them

    by_geometry : str, list
        .. versionadded:: 0.2.10
        
        The passed argument can be a WKT (string) or a list of numbers. If three
        numbers are passed, this is interpreted as a center point and a buffer 
        distance in meter. If four numbers are passed, this is a bounding box.
        If a 2D-list of lists is passed (with two numbers), this will be used to
        construct a search Polygon. 
        Finally the constructed geometry is used to apply a spatial filter to the 
        results.
    as_result : bool
        If True, the reuslts will be merged into a
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>`.
        Defaults to False. Will be ignored if return_iterator
        is True
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.

    Returns
    -------
    records : list
        List of matched Entry or ImmutableResultSet.

    """
    # handle uuid first
    if uuid is not None:
        query = session.query(models.Entry).filter(models.Entry.uuid==uuid)
        if return_iterator:
            return query
        else:
            return query.first()

    # base query
    query = session.query(models.Entry)

    # check for partial entries:
    if not include_partial:
        query = query.filter(models.Entry.is_partial == false())

    # make this an option
    if version == 'latest':
        query = query.filter(models.Entry.latest_version_id.is_(None))
        version = None

    # now the remaining parameters
    if id is not None:
        query = query.filter(models.Entry.id==id)
    if title is not None:
        query = query.filter(_match(models.Entry.title, title))
    if abstract is not None:
        query = query.filter(_match(models.Entry.abstract, abstract))
    if external_id is not None:
        query = query.filter(_match(models.Entry.external_id, external_id))
    if version is not None:
        query = query.filter(models.Entry.version==version)
    
    # location
    if by_geometry is not None:
        # get the search area
        if isinstance(by_geometry, (list, tuple, np.ndarray)) and len(by_geometry) == 3:
            area = location.get_search_shape([by_geometry[0], by_geometry[1]], buffer=by_geometry[2])
        else:
            area = location.get_search_shape(by_geometry)
        
        # append the spatial filter
        query = location.build_query(query, area)

    # -------------------------------------
    # some second level lookups
    # -------------------------------------

    # license
    if license is not None:
        if isinstance(license, models.License):
            license = license.id
        if isinstance(license, int):
            query = query.filter(models.Entry.license_id==license)
        elif isinstance(license, str):
            query = query.join(models.License).filter(_match(models.License.short_title, license))
        else:
            raise AttributeError('license has to be int or str.')

    # variable
    if variable is not None:
        if isinstance(variable, models.Variable):
            variable = variable.id
        if isinstance(variable, int):
            query = query.filter(models.Entry.variable_id==variable)
        elif isinstance(variable, str):
            query = query.join(models.Variable).filter(_match(models.Variable.name, variable))
        else:
            raise AttributeError('variable has to be int or str.')

    # project
    if project is not None:
        if isinstance(project, models.EntryGroup):
            if project.type.name != 'Project':
                raise TypeError("EntryGroup has to be of type 'Project'.")
            project = project.id
        if isinstance(project, int):
            join = query.join(models.EntryGroupAssociation).join(models.EntryGroup).join(models.EntryGroupType)
            query = join.filter(models.EntryGroupType.name=='Project').filter(models.EntryGroup.id==project)
        elif isinstance(project, str):
            join = query.join(models.EntryGroupAssociation).join(models.EntryGroup).join(models.EntryGroupType)
            query = join.filter(models.EntryGroupType.name=='Project').filter(_match(models.EntryGroup.title, project))
        else:
            raise AttributeError('project has to be int or str')

    # first author
    if author is not None:
        if isinstance(author, models.Person):
            author = author.id
        if isinstance(author, int):
            join = query.join(models.PersonAssociation).join(models.PersonRole).join(models.Person)
            query = join.filter(models.PersonRole.name=='author').filter(models.Person.id==author)
        elif isinstance(author, str):
            join = query.join(models.PersonAssociation).join(models.PersonRole).join(models.Person)
            query = join.filter(models.PersonRole.name=='author').filter(
                (_match(models.Person.first_name, author)) | (_match(models.Person.last_name, author))
            )
        else:
            raise AttributeError('author has to be int or str')

    # contributor
    if contributor is not None or coauthor is not None:

        # TODO clean this all up when the contributor behavior is changed
        if not os.getenv('METACATALOG_SUPRESS_WARN', False) and contributor is not None:
            warnings.warn("The contributor argument will change with a future release. Contributors will be kept, but filter for **any** associated person with a future release. If you want to keep the current behavior, use the authors argument. To supress this warning set the METACATALOG_SUPRESS_WARN environment variable.", FutureWarning)
        if coauthor is not None:
            contributor = coauthor

        if isinstance(contributor, models.Person):
            contributor = contributor.id
        if isinstance(contributor, int):
            join = query.join(models.PersonAssociation).join(models.PersonRole).join(models.Person)
            query = join.filter(models.PersonRole.name.in_(['author', 'coAuthor'])).filter(models.Person.id==contributor)
        elif isinstance(contributor, str):
            join = query.join(models.PersonAssociation).join(models.PersonRole).join(models.Person)
            query = join.filter(models.PersonRole.name.in_(['author', 'coAuthor'])).filter(
                (_match(models.Person.first_name, contributor)) | (_match(models.Person.last_name, contributor))
            )
        else:
            raise AttributeError('contributior has to be int or str')

    # keywords
    if keywords is not None:
        query = query.join(models.KeywordAssociation).join(models.Keyword)
        if not isinstance(keywords, (list, tuple)):
            keywords = [keywords]
        # for every keyword
        for keyword in keywords:
            if isinstance(keyword, models.Keyword):
                keyword = keyword.id
            if isinstance(keyword, int):
                query = query.filter(models.Keyword.id==keyword)
            elif isinstance(keyword, str):
                query = query.filter(_match(models.Keyword.value, keyword))
            else:
                raise AttributeError('keywords have to be a list of int or str')

    # details
    if details is not None:
        if not isinstance(details, dict):
            raise TypeError('The details have to be given as a dictionary')

        # build the query
        query = query.join(models.Detail)

        # build a stemmer
        ps = nltk.PorterStemmer()

        for key, value in details.items():
            query = query.filter(_match(models.Detail.stem, ps.stem(key)))
            
            # handle nested json data
            if isinstance(value, (list, tuple, dict)):
                query = query.filter(models.Detail.raw_value.contains(value))
            else:
                query = query.filter(_match(models.Detail.raw_value['__literal__'].astext, str(value)))

    # return
    if return_iterator:
        return query
    else:
        if as_result:
            all_irs = [ImmutableResultSet(entry) for entry in query.all()]
            results = []
            
            # do not return empty or duplicates results
            for irs in all_irs:
                if irs.empty or irs.checksum in [r.checksum for r in results]:
                    continue
                else:
                    results.append(irs)
            return results
            
        return query.all()
