"""SHOW API
The Show API endpoint can be used to pretty print database records 
or show the database structure, like attributes.
"""
from ._mapping import TABLE_MAPPING, ENTITY_MAPPING


def __infer_model_from_name(table_name):
    name = table_name.lower()
    if name in TABLE_MAPPING.keys():
        Model = TABLE_MAPPING[name]
    elif name in ENTITY_MAPPING.keys():
        Model = ENTITY_MAPPING[name]
    else:
        raise ValueError("An entity '%s' is not known" % table_name)
    
    return Model

def show_attributes(table_name, add_type=False):
    """SHOW attributes

    Returns a list of available attributes on the given table.
    The table_name has to match the actual table in the database.

    Params
    ------
    table_name : str
        Name of the table the attributes are requested for.
    add_type : bool
        If True, a list of tuples will be returned. The tuple will
        contain (column_table, colmun_data_type)

    Returns
    -------
    attributes : list
        List of attributes available on the requested table.

    """
    Model = __infer_model_from_name(table_name)

    if add_type:
        return [(col.name, col.type) for col in Model.__table__.columns]
    else:
        return [col.name for col in Model.__table__.columns]


def show_records(session, table_name, limit=None, where=None, as_dict=True):
    """SHOW records

    Returns a list or dictionary of raw table contents in 
    the database. 

    Parameters
    ----------
    session : sqlalchemy.Session
        SQLAlchemy session connected to the database.
    table_name : str
        Name of the table the records are requested for.
    limit : int
        Positive integer to limit the requested data.
        If None, no limit will be set
    where : str
        Possible raw SQL WHERE clause. If None, the 
        output will not be filtered.
    as_dict : bool
        If True, each records will be returned as a dict,
        else as a tuple with the columns ordered as 
        defined in the Model

    Returns
    -------
    records : list
        List of dicts or tuples representing the records.
    """
    # get the model 
    Model = __infer_model_from_name(table_name)
    attributes = show_attributes(table_name=table_name, add_type=False)

    # get the data
    sql = ' select %s from %s' % (', '.join(['"%s"' % a for a in attributes]), Model.__tablename__)
    
    # add where
    if where is not None:
        sql += ' WHERE ' + where
    
    # add limit
    if limit is not None:
        sql += ' LIMIT %d' % limit

    try:
        result = session.execute(sql)
    except Exception as e:
        session.rollback()
        raise e

    if as_dict:
        return [{k:v for k,v in zip(attributes, record)} for record in result]
    else:
        return [tuple(record) for record in result]
