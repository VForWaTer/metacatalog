"""SHOW API
The Show API endpoint can be used to pretty print database records 
or show the database structure, like attributes.
"""
from ._mapping import TABLE_MAPPING


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
    Model = TABLE_MAPPING[table_name]

    if add_type:
        return [(col.name, col.type) for col in Model.__table__.columns]
    else:
        return [col.name for col in Model.__table__.columns]
