"""FIND operation

A find operation returns objects from the metacatalog on exact matches.
At the current stage, the following objects can be found by a FIND operation:

* keywords

"""
from metacatalog import models
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import false

def _match(column_instance: InstrumentedAttribute, compare_value: str, invert=False) -> BinaryExpression:
    """
    Create Column based Compare logic

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


def find_keyword(session, id=None, uuid=None, value=None, thesaurus_name=None, return_iterator=False):
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
    uuid : str
        .. versionadded:: 0.1.13
        Find by version 4 UUID. If uuid is given, all other options
        will be ignored. 
    value : str
        Value of the requested keyword(s). Multiple record
        return is possible.
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
    if value is not None:
        query = query.filter(_match(models.Keyword.value, value))
    if thesaurus_name is not None:
        query = query.filer(_match(models.Keyword.thesaurusName.name, thesaurus_name))

    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_thesaurus(session, id=None, name=None, title=None, organisation=None, description=None, return_iterator=False):
    """Find Thesaurii
    ..versionadded:: 0.1.10

    Retun one or many thesaurii references from the database 
    on exact matches. You can  use `'%'` and `'*'` as wildcards 
    and prepend a str with `!` to invert the filter.

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    id : integer
        Database unique ID of the requested record. Will 
        return only one record.
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


def find_license(session, id=None, title=None, short_title=None, by_attribution=None, share_alike=None, commercial_use=None, return_iterator=False):
    """Find license

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


def find_unit(session, id=None, name=None, symbol=None, return_iterator=False):
    """Find Unit

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


def find_variable(session, id=None, name=None, symbol=None, return_iterator=False):
    """Find Variable

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


def find_datasource_type(session, id=None, name=None, return_iterator=False):
    """Find Datasource Type

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


def find_role(session, id=None, name=None, return_iterator=False):
    """Find Person Role

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


def find_person(session, id=None, first_name=None, last_name=None, role=None, organisation_name=None, return_iterator=False):
    """Find Person

    Return person record on exact matches. Persons can be 
    identified by id, first_name, last_name or associated roles.

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
    
    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_group_type(session, id=None, uuid=None, name=None, return_iterator=False):
    """Find entry group types

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


def find_group(session, id=None, uuid=None, title=None, type=None, return_iterator=False):
    """Find group

    Find a group of entries on exact matches. Groups can be 
    identified by id, title or its type.

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
    
    Returns
    -------
    records : list of metacatalog.EntryGroupType
        List of matched EntryGroupType instance. 

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
        return query.all()


def find_entry(session, id=None, uuid=None, title=None, abstract=None, external_id=None, version='latest', return_iterator=False):
    """Find Entry

    Find an meta data Entry on exact matches. Entries can be 
    identified by id, title, external_id and version. The 
    version can be added to all other matching types, which 
    are mutually exclusive.

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
        
        .. code-block:: python
            api.find_entry(session, abstract='*phrase to find*')
        
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
    # handle uuid first
    if uuid is not None:
        query = session.query(models.Entry).filter(models.Entry.uuid==uuid)
        if return_iterator:
            return query
        else:
            return query.first()

    # base query
    query = session.query(models.Entry).filter(models.Entry.is_partial == false())

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

    # return
    if return_iterator:
        return query
    else:
        return query.all()
