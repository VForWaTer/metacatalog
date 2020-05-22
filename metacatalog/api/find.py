"""FIND operation

A find operation returns objects from the metacatalog on exact matches.
At the current stage, the following objects can be found by a FIND operation:

* keywords

"""
from metacatalog import models


def find_keyword(session, id=None, value=None, return_iterator=False):
    """Find keyword

    Return one or many keyword entries from the database on 
    exact matches.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will 
        return only one record.
    value : str
        Value of the requested keyword(s). Multiple record
        return is possible.
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

    # add needed filter
    if id is not None:
        query = query.filter(models.Keyword.id==id)
    if value is not None:
        query = query.filter(models.Keyword.value==value)
    
    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_license(session, id=None, short_title=None, by_attribution=None, share_alike=None, commercial_use=None, return_iterator=False):
    """Find license

    Return one or many license entries from the database on 
    exact matches.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will 
        return only one record.
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
    if short_title is not None:
        query = query.filter(models.License.short_title==short_title)
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


def find_unit(session, id=None, name=None, symbol=None, return_iterator=False):
    """Find Unit

    Return one unit entry from the database on 
    exact matches. It makes only sense to set one of the 
    attributes (id, name, symbol).

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
        query = query.filter(models.Unit.name==name)
    if symbol is not None:
        query = query.filter(models.Unit.symbol==symbol)

    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_variable(session, id=None, name=None, symbol=None, return_iterator=False):
    """Find Variable

    Return one vriable entry from the database on 
    exact matches. It makes only sense to set one of the 
    attributes (id, name, symbol).

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
        query = query.filter(models.Variable.name==name)
    if symbol is not None:
        query = query.filter(models.Variable.symbol==symbol)

    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_datasource_type(session, id=None, name=None, return_iterator=False):
    """Find Datasource Type

    Return one datasource type record on exact matches. 
    Types can be identified by id or name.

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
        query = query.filter(models.DataSourceType.name==name)

    # return 
    if return_iterator:
        return query
    else: 
        return query.all()



def find_role(session, id=None, name=None, return_iterator=False):
    """Find Person Role

    Return one person role record on exact matches. 
    Roles can be identified by id or name.

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
        query = query.filter(models.PersonRole.name==name)

    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_person(session, id=None, first_name=None, last_name=None, role=None, return_iterator=False):
    """Find Person

    Return person record on exact matches. Persons can be 
    identified by id, first_name, last_name or associated roles.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will 
        return only one record.
    first_name : str
        First name attribute of the requested person. 
    last_name : str
        Last name attribute of the requested person.
    role : int, str
        Role id or name (exact match) that is associated to 
        a person. Will most likely return many persons.
    return_iterator : bool
        If True, an iterator returning the requested objects 
        instead of the objects themselves is returned.
    
    Returns
    -------
    records : list of metacatalog.Person
        List of matched Person instance. 

    """
    # base query
    query = session.query(models.Person)

    if id is not None:
        query = query.filter(models.Person.id==id)
    
    if first_name is not None:
        query = query.filter(models.Person.first_name==first_name)
    
    if last_name is not None:
        query = query.filter(models.Person.last_name==last_name)
    
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

    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_group_type(session, id=None, name=None, return_iterator=False):
    """Find entry group types

    Find a group type on exact matches. The group types
    describes a collection of entries. 

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
        query = query.filter(models.EntryGroupType.name==name)

    # return 
    if return_iterator:
        return query
    else:
        return query.all()


def find_group(session, id=None, title=None, type=None, return_iterator=False):
    """Find group

    Find a group of entries on exact matches. Groups can be 
    identified by id, title or its type.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will 
        return only one record.
    title : str
        Title attribute of the group.
    type : int, str
        Either the id or name of a group type to exact match.
    return_iterator : bool
        If True, an iterator returning the requested objects 
        instead of the objects themselves is returned.
    
    Returns
    -------
    records : list of metacatalog.EntryGroupType
        List of matched EntryGroupType instance. 

    """
    # base query
    query = session.query(models.EntryGroup)

    if id is not None:
        query = query.filter(models.EntryGroup.id==id)
    if title is not None:
        query = query.filter(models.EntryGroup.title==title)
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
        return query.all()


def find_entry(session, id=None, title=None, external_id=None, version=None, return_iterator=False):
    """Find Entry

    Find an meta data Entry on exact matches. Entries can be 
    identified by id, title, external_id and version. The 
    version can be added to all other matching types, which 
    are mutually exclusive.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will 
        return only one record.
    title : str
        Title attribute of the Entry.
    external_id : str
        External id attrinbute of the Entry.
    version : int
        Version number of the Entry. Can be combined with 
        the other matching parameters, as they might not be 
        different between versions.
    return_iterator : bool
        If True, an iterator returning the requested objects 
        instead of the objects themselves is returned.
    
    TODO
    ----
    if version is None, use the lastest version

    Returns
    -------
    records : list of metacatalog.Entry
        List of matched Entry instance. 
    """
    # base query
    query = session.query(models.Entry)

    if id is not None:
        query = query.filter(models.Entry.id==id)
    if title is not None:
        query = query.filter(models.Entry.title==title)
    if external_id is not None:
        query = query.filter(models.Entry.external_id==external_id)
    if version is not None:
        query = query.filter(models.Entry.version==version)

    # return
    if return_iterator:
        return query
    else:
        return query.all()
