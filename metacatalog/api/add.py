"""ADD API

The ADD API endpoint can be used to add records to the database.
"""
from uuid import uuid4

from ._mapping import TABLE_MAPPING
from metacatalog import api
from metacatalog import models

def _add(session, InstanceModel, **kwargs):
    """
    Common method for inserting a new record. 
    Should not be used directly.
    """
    entity = InstanceModel(**kwargs)

    try:
        session.add(entity)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

    #return the added entity
    return entity


def add_record(session, tablename, **kwargs):
    """
    Method to map tablenames to InstanceModels. 
    Should not be used directly.
    """
    if not tablename in TABLE_MAPPING.keys():
        raise ValueError('A table of name %s is not known.' % tablename)
    else:
        return _add(session=session, InstanceModel=TABLE_MAPPING.get(tablename), **kwargs)


def add_license(session, short_title, title, **kwargs):
    """Add license record

    Add a new License record to the database. 

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    short_title : str
        Short title of the License, max 40 letters.
    title : str
        Full title of the License.
    summary : str
        Gives a short summary of the key points for 
        the given license.
    full_text : str
        Full legal code of the license, if available.
    link : str
        Link to an online resource of the license.
    by_attribution : bool
        Does the license require author attribution on 
        distribution? Defaults to True.
    share_alike : bool
        Does the license require redistributions to be 
        licensed under the same license? Defaults to True.
    commercial_use : bool
        Does the license permit commercial use of the 
        resource? Defaults to True

    Return
    ------
    license : metacatalog.License
        License instance of the added license entity.
    """
    # add the mandatory attributes
    kwargs['short_title'] = short_title
    kwargs['title'] = title

    # add the license
    return add_record(session=session, tablename='licenses', **kwargs)


def add_unit(session, name, symbol, si=None):
    """Add unit record

    Add a new unit to the database

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    name : str
        The unit name. Max 64 letters.
    symbol : str
        The unit symbol. Try to use the correct 
        physical unit symbols. Max 12 letters.
    si : str
        SI representation of the unit. Can be 
        omitted.
    
    Returns
    -------
    unit : metacatalog.Unit
        Unit instance of the added unit entity
    """
    # create the attribute dict
    attrs = dict(name=name, symbol=symbol)
    if si is not None:
        attrs['si'] = si

    # add the unit
    return add_record(session=session, tablename='units', **attrs)


def add_variable(session, name, symbol, unit):
    r"""Add variable record

    Add a new variable to the database.

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    name : str
        The variable name. Max 64 letters.
    symbol : str
        The variable symbol. Try to use the correct 
        physical variable symbols and avoid dublicates.
    unit : int, str
        Either the id or **full** name of the unit to be 
        linked to this variable.

    Returns
    -------
    variable : metacatalog.Variables
        Variable instance of the added variable entity

    """
    #create the attribute dict
    attrs = dict(name=name, symbol=symbol)

    # get the unit
    if isinstance(unit, int):
        unit = api.find_unit(session=session, id=unit, return_iterator=True).one()
    elif isinstance(unit, str):
        unit = api.find_unit(session=session,name=unit, return_iterator=True).first()
    else:
        raise AttributeError('unit has to be of type integer or string.')

    attrs['unit_id'] = unit.id

    # add the variable
    return add_record(session=session, tablename='variables', **attrs)


def add_keyword(session, path):
    r"""Add Keyword

    Add a new keyword to the database. The keyword is
    added by the full path.

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    path : str
        A full path to the keyword, each element connected
        by a ' > ' sequence. E.g.: 
        Topic > Term > Variable_level_1 etc.

    Returns
    -------
    keywords, list of metacatalog.Keywords
        List of the deconstructed Keyword entities    

    """
    levels = path.upper().split(' > ')
    current_parent_id = None
    keywords = []

    # add each level, if it does not exist
    for i, level in enumerate(levels):
        # does the level exist?
        current = session.query(models.Keyword).filter(models.Keyword.value==level).filter(models.Keyword.parent_id==current_parent_id).first()
        if current is None:
            attr = dict(parent_id=current_parent_id, value=level, uuid=uuid4(), path=' > '.join(levels[:i + 1]))
            current = add_record(session=session, tablename='keywords', **attr)
        keywords.append(current)
        current_parent_id = current.id
    
    # return 
    return keywords


def add_person(session, first_name, last_name, affiliation=None):
    r"""Add new Person

    Add a new Person to the database. A person can be a real Person
    or an institution. Then, the institution name goes into the 
    last_name column and first_name can actively be set to None.
    An affiliation can be added to both as a single string.

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    first_name : str
        A real persons first name. If omitted, the 'Person' is 
        assumed to be an institution
    last_name : str
        A real persions last name. If first_name is NULL, 
        last_name is assumned to be an institution.
    affiliation : str
        Affiliation if applicable. Has to go into a single string
        of 1024 bytes.
    
    Returns
    -------
    entry: metacatalog.Person
        Entry instance of the added Person entity
  
    """
    attr = dict(first_name=first_name, last_name=last_name, affiliation=affiliation)

    return add_record(session=session, tablename='persons', **attr)


def add_entry(session, title, author, location, variable, abstract=None, external_id=None, geom=None, license=None, embargo=False, **kwargs):
    r"""Add new Entry

    Adds a new metadata Entry to the database. This method will create the core
    entry. Usually, more steps are necessary, which will need the newly created 
    database ID. Such steps are: 
    
    * adding contributors   (mandatory)
    * adding data           (extremly useful)
    * adding keywords       (recommended)

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    title : str
        Title of the Entry
    author : int, str
        First author of the Entry. The Person record has to exist already in the 
        database and can be found by exact match on id (int) or last_name (str).
    location : str, tuple
        Can be either a WKT of a EPSG:4326 location, or the coordinates as a 
        tuple. It has to be (X,Y), to (longitude, latitude)
    variable : int, str
        **Full** variable name (str) or ID (int) of the data described by the Entry. 
    abstract : str
        Description of the data. Be as detailed as possible
    external_id : str
        If the data described by Entry has another unique identifier, 
        usually supplied by the data provider, it can be stored for reference reasons.
    comment : str
        General purpose comment that should not contain any vital information to 
        understand the entry. If it's vital, it should go into the abstract.
    geom : str
        WKT of any additional geoinformation in EPSG:4326
    license : str, int
        Either the id or **full** name of the license to be linked to this Entry.
    embargo : bool
        If True, this Entry will **not** be publicly available until the embargo ends
        The embargo period is usually 2 years but can be modified using the kwargs.
    Returns
    -------
    entry: metacatalog.Entry
        Entry instance of the added entry entity


    """
    # create the attribute dict
    attr = dict(
        title=title, 
        abstract=abstract, 
        external_id=external_id,
        embargo=embargo
    )
    attr.update(kwargs)

    # parse the author
    if isinstance(author, int):
        author = api.find_person(session=session, id=author, return_iterator=True).one()
    elif isinstance(author, str):
        author = api.find_person(session=session, last_name=author, return_iterator=True).first()
    else:
        raise AttributeError('author has to be of type int or str')

    # parse the location and geom
    if isinstance(location, str):
        attr['location'] = location
    elif isinstance(location, (tuple, list)):
        attr['location'] = 'POINT (%f %f)' % (location[0], location[1])

    if geom is not None and isinstance(geom, str):
        attr['geom'] = geom
    
    # handle variable
    if isinstance(variable, int):
        variable = api.find_variable(session=session, id=variable, return_iterator=True).one()
    elif isinstance(variable, str):
        variable = api.find_variable(session=session, name=variable, return_iterator=True).first()
    else:
        raise AttributeError('variable has to be of type integer or string.')
    attr['variable_id'] = variable.id

    # handle license
    if isinstance(license, int):
        license = api.find_license(session=session, id=license, return_iterator=True).one()
    elif isinstance(license, str):
        license = api.find_license(session=session, short_title=license, return_iterator=True).first()
    if license is not None:
        attr['license_id'] = license.id

    # add the entry
    entry = add_record(session=session, tablename='entries', **attr)

    # reference the person using 'First Author' (ID=1) Role
    add_persons_to_entries(session, entry, author, 1, 1)

    return entry

def add_details_to_entries(session, entries, **kwargs):
    """Associate detail(s) to entrie(s)

    Add key-value pair details to one, or many Entry(s).
    The Entry(s) have to already exist in the database.

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    entries : list
        List of identifier or single identifier to load entries. 
        If int, the Entry.id is assumed. If str, title is assumed.
        Can also pass a metacatalog.Entry object. 
    kwargs : keyword arguments
        Each keyword argument will be added as a 
        py:class:`metacatalog.models.Detail` and linked to 
        each entry

    """
    # check the input shapes
    if not isinstance(entries, list):
        entries = [entries]

    # add for each entry
    for entry_id in entries:
        # load the entry
        if isinstance(entry_id, models.Entry):
            entry = entry_id
        elif isinstance(entry_id, int):
            # TODO sort by version descending to get the lastest
            entry = api.find_entry(session=session, id=entry_id, return_iterator=True).first()
        elif isinstance(entry_id, str):
            # TODO sort by version descending to get the lastest
            entry = api.find_entry(session=session, title=entry_id, return_iterator=True).first()
        else:
            raise AttributeError("Value '%s' not allowed for entries" % str(type(entry_id)))

        # add the details
        entry.add_details(**kwargs)
    
        # try to commit
        try:
            session.add(entry)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e


def add_keywords_to_entries(session, entries, keywords, alias=None, values=None):
    r"""Associate keyword(s) to entrie(s)

    Adds associations between entries and keywords. The Entry and Keyword
    instances have to already exist in the database. Keywords are usually 
    prepopulated. You might want to alias an keyword or associate a value to 
    it. Use the alias and value lists for this.

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    entries : list
        List of identifier or single identifier to load entries. 
        If int, the Entry.id is assumed. If str, title is assumed.
        Can also pass a metacatalog.Entry object. 
    keywords : list
        List of identifier or single identifier to load keywords.
        If int, Keyword.id is assumed, If str, Keyword.value is assumed.
        Can also pass a metacatalog.Keyword object.
    alias : list
        List of, or single alias names. The shape has to match the 
        keywords parameter. These alias will rename the keywords on 
        association. In case one instance should not recive an alias, 
        pass None instead.
    values : list
        List of, or single value. The shape has to match the 
        keywords parameter. These values will be stored along with the
        association to the entries. In case one instance should not 
        be associated to a value pass None instead.

        .. deprecated:: 0.1.6
            `values` will be removed in 0.2. Rather use 
            `metacatalog.models.Detail` to store custom key-values 


    Returns
    -------
    void

    See Also
    --------
    metacatalog.Entry
    metacatalog.Keyword

    """
    # check the input shapes
    if not isinstance(entries, list):
        entries = [entries]
    if not isinstance(keywords, list):
        keywords = [keywords]
    if not isinstance(alias, list):
        alias = [alias] * len(keywords)
    if not isinstance(values, list):
        values = [values] * len(keywords)

    # add for each entry
    for entry_id in entries:
        # load the entry
        if isinstance(entry_id, models.Entry):
            entry = entry_id
        elif isinstance(entry_id, int):
            # TODO sort by version descending to get the lastest
            entry = api.find_entry(session=session, id=entry_id, return_iterator=True).first()
        elif isinstance(entry_id, str):
            # TODO sort by version descending to get the lastest
            entry = api.find_entry(session=session, title=entry_id, return_iterator=True).first()
        else:
            raise AttributeError("Value '%s' not allowed for entries" % str(type(entry_id)))
        
        # add each keyword
        assocs = []
        for keyword_id, alias_name, value in zip(keywords, alias, values):
            # load the keyword
            if isinstance(keyword_id, models.Keyword):
                keyword = keyword_id
            elif isinstance(keyword_id, int):
                keyword = api.find_keyword(session=session, id=keyword_id, return_iterator=True).first()
            elif isinstance(keyword_id, str):
                keyword = api.find_keyword(session=session, value=keyword_id, return_iterator=True).first()
            else:
                raise AttributeError("Value '%s' not allowed for keywords" % str(type(keyword_id)))
        
            # create a new keyword association
            assocs.append(models.KeywordAssociation(entry=entry, keyword=keyword, alias=alias_name, associated_value=value))
        
        # add keyword to current entry
        try:
            entry.keywords.extend(assocs)
            session.add(entry)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e


def add_persons_to_entries(session, entries, persons, roles, order):
    r"""Add person(s) to entrie(s)

    Adds associations between entries and persons. The Entry and Person
    instances have to already exist in the database. Each association 
    has to further define the role of the person for the respective entry.

    Parameters
    ----------
   session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    entries : list
        List of identifier or single identifier to load entries. 
        If int, the Entry.id is assumed. If str, title is assumed.
        Can also pass a metacatalog.Entry object. 
    persons : list
        List of identifier or single identifier to load persons.
        If int, Person.id is assumed, If str, Person.last_name is assumed.
        Can also pass a metacatalog.Person object.
    roles : list
        List of, or single role. The shape has to match the 
        persons parameter. The role has to be identifies by id (int) or
        role name (str).
    order : list
        List of, or single order. The shape has to match the 
        persons parameter. The order gives the ascending order of 
        contributors on the respecive entry (after the author).

    Returns
    -------
    void

    See Also
    --------
    metacatalog.Entry
    metacatalog.Person
    metacatalog.PersonRole
    
    """
    # check the input shapes
    if not isinstance(entries, list):
        entries = [entries]
    if not isinstance(persons, list):
        persons = [persons]
    if not isinstance(roles, list):
        roles = [roles] * len(persons)
    if not isinstance(order, list):
        order = [order] * len(persons)

    # add for each entry
    for entry_id in entries:
        # load the entry
        if isinstance(entry_id, models.Entry):
            entry = entry_id
        elif isinstance(entry_id, int):
            # TODO sort by version descending to get the lastest
            entry = api.find_entry(session=session, id=entry_id, return_iterator=True).first()
        elif isinstance(entry_id, str):
            # TODO sort by version descending to get the lastest
            entry = api.find_variable(session=session, title=entry_id, return_iterator=True).first()
        else:
            raise AttributeError("Value '%s' not allowed for entries" % str(type(entry_id)))

        # add each person
        assocs = []
        for person_id, role_id, order_num in zip(persons, roles, order):
            # load the person
            if isinstance(person_id, models.Person):
                person = person_id
            elif isinstance(person_id, int):
                person = api.find_person(session=session, id=person_id, return_iterator=True).one()
            elif isinstance(person_id, str):
                person = api.find_person(session=session, last_name=person_id, return_iterator=True).first()
            else:
                raise AttributeError('Persons can only be identified by id or last_name')

            # load the role
            if isinstance(role_id, models.PersonRole):
                role = role_id
            elif isinstance(role_id, int):
                role = api.find_role(session=session, id=role_id, return_iterator=True).one()
            elif isinstance(role_id, str):
                role = api.find_role(session=session, name=role_id, return_iterator=True).first()
            else:
                raise AttributeError('Roles can only be identified by id or name')

            # create the new association
            assocs.append(models.PersonAssociation(entry=entry, person=person, role=role, order=order_num))


        # add each person to entry
        try:
            entry.contributors.extend(assocs)
            session.add(entry)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e