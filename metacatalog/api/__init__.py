"""
Importing
---------
The `metacatalog.api` submoule contains all API functions that are 
available for Python. The CLI also relies on the API and therefore using the 
Api is the recommended way to use metacatalog.

It is recommended to import the API like:

.. code-block:: python
    
    from metacatalog import api

If you have other modules present that are called ``api``, it is recommended
to import the API like:

.. code-block:: python

    from metacatalog import api as mc_api

Functions
---------

There are three functions that are used to connect to the database and
create the necessary tables to setup metacatalog These are:

* :py:func:`connect_database`
* :py:func:`create_tables`
* :py:func:`populate_defaults`

All other API functions follow a similar pattern. Each of them expect a 
:class:`Session <sqlalchemy.orm.session.Session>` as the first argument
(See :py:func:`connect_database`). 

The names of all functions in `metacatalog.api` are prefixed with an 
*action* identifier:

* ``find_*`` to find entities on exact matches and filters
* ``add_*`` to add new entitites
* ``show_*`` to inspect raw table records

"""
from .db import connect_database, create_tables, populate_defaults, update_sequence
from .find import (
    find_keyword,
    find_thesaurus,
    find_license, 
    find_unit, 
    find_variable,
    find_datasource_type,
    find_role, 
    find_person, 
    find_group_type, 
    find_group, 
    find_entry
)
from .show import show_attributes, show_records
from .io import from_csv, from_text, from_json
from .add import (
    add_license, 
    add_keyword,
    add_thesaurus, 
    add_unit, 
    add_variable,
    add_person, 
    add_entry,
    add_group,
    add_project,
    add_details_to_entries,
    add_keywords_to_entries,
    add_persons_to_entries
)
from .catalog import get_uuid
