"""FIND operation

A find operation returns objects from the metacatalog on exact matches.
At the current stage, the following objects can be found by a FIND operation:

* keywords

"""
from metacatalog import models
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.expression import false, true

import nltk

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


def find_person(session, id=None, uuid=None, first_name=None, last_name=None, role=None, organisation_name=None, organisation_abbrev=None, return_iterator=False):
    """Find Person

    Return person record on exact matches. Persons can be
    identified by id, first_name, last_name, organisation details or associated roles.
    Since version ``0.2.5`` only Persons which have a ``is_organisation==False``
    will be returned

    .. versionchanged:: 0.1.8
        string matches now allow `'%'` and `'*'` wildcards and can
        be inverted by prepending `!`

    .. versaionchanged:: 0.2.6
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

    # return
    if return_iterator:
        return query
    else:
        return query.all()


def find_organisation(session, id=None, organisation_name=None, organisation_abbrev=None, role=None,  return_iterator=False):
    """Find Organisation
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


def find_entry(session, id=None, uuid=None, title=None, abstract=None, license=None, variable=None, external_id=None, version='latest', project=None, author=None, contributor=None, keywords=None, details=None, return_iterator=False):
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
    license : str, int
        .. versionadded:: 0.2.2
        The license can be a :class:``License <metacatalog.models.License>`,
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
        The project can be a :class:`EntryGroup <metacatalog.models.EntryGroup>` of
        :class:`EntryGroupType.name=='Project' <metacatalog.models.EntryGroupType>`,
        its id (int) or title (str)
    author : int, str
        .. versionadded:: 0.2.2
        The author can be a :class:`Person <metacatalog.models.Person>`,
        his id (int) or name (str). A string argument will match first and last
        names. The author is only the first author. For other contributors see
        :attr:`contributor`.
    contributor : int, str
        .. versionadded:: 0.2.2
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
    return_iterator : bool
        If True, an iterator returning the requested objects
        instead of the objects themselves is returned.


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
    if contributor is not None:
        if isinstance(contributor, models.Person):
            contributor = contributor.id
        if isinstance(contributor, int):
            join = query.join(models.PersonAssociation).join(models.PersonRole).join(models.Person)
            query = join.filter(models.PersonRole.name.in_('author', 'coAuthor')).filter(models.Person.id==contributor)
        elif isinstance(contributor, str):
            join = query.join(models.PersonAssociation).join(models.PersonRole).join(models.Person)
            query = join.filter(models.PersonRole.name.in_('author', 'coAuthor')).filter(
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
        # build the query
        query = query.join(models.Detail)

        # build a stemmer
        ps = nltk.PorterStemmer()

        if not isinstance(details, dict):
            raise TypeError('The details have to be given as a dictionary')
        for key, value in details.items():
            query = query.filter(models.Detail.stem==ps.stem(key)).filter(_match(models.Detail.value, value))

    # return
    if return_iterator:
        return query
    else:
        return query.all()
