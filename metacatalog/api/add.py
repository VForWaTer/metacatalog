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
    """Add variable record

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
    """Add Keyword

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
