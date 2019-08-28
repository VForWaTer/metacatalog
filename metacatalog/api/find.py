"""FIND operation

A find operation returns objects from the metacatalog on exact matches.
At the current stage, the following objects can be found by a FIND operation:

* keywords

"""
from metacatalog import Keyword, License, Unit, Variable


def find_keyword(session, id=None, value=None, return_iterator=False):
    """Find keyword

    Return one or many keyword entries from the database on 
    exact matches.

    Params
    ------
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
    query = session.query(Keyword)

    # add needed filter
    if id is not None:
        query = query.filter(Keyword.id==id)
    if value is not None:
        query = query.filter(Keyword.value==value)
    
    # return
    if return_iterator:
        return query
    else:
        return query.all()



def find_license(session, id=None, short_title=None, by_attribution=None, share_alike=None, commercial_use=None, return_iterator=False):
    """Find license

    Return one or many license entries from the database on 
    exact matches.

    Params
    ------
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
    query = session.query(License)

    # add needed filter
    if id is not None:
        query = query.filter(License.id==id)
    if short_title is not None:
        query = query.filter(License.short_title==short_title)
    if by_attribution is not None:
        query = query.filter(License.by_attribution==by_attribution)
    if share_alike is not None:
        query = query.filter(License.share_alike==share_alike)
    if commercial_use is not None:
        query = query.filter(License.commercial_use==commercial_use)
    
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

    Params
    ------
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
    query = session.query(Unit)

    if id is not None:
        query = query.filter(Unit.id==id)
    if name is not None:
        query = query.filter(Unit.name==name)
    if symbol is not None:
        query = query.filter(Unit.symbol==symbol)

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

    Params
    ------
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
    query = session.query(Variable)

    if id is not None:
        query = query.filter(Variable.id==id)
    if name is not None:
        query = query.filter(Variable.name==name)
    if symbol is not None:
        query = query.filter(Variable.symbol==symbol)

    # return
    if return_iterator:
        return query
    else:
        return query.all()
