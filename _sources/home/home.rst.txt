====
Home
====

.. toctree::
    :maxdepth: 1
    :hidden:

    install
    getting_started

How the docs work
=================

Metacatalog is a management tool for a PostgreSQL/PostGIS database that is 
primarily used to store meta-data of environmental open data. While it can 
also store the data itself, it is more meant as a Meta-database that should
rather interface the original data stores.

Metacatalog has three main submodules:

* the `Database Models <../models/models.rst>`_
* a Python `API <../api/api.rst>`_ 
* a `CLI <../cli/cli.rst>`_

Please refer to each section to learn more about each submodules.
In general terms, the Models give you great freedom in adding, editing and 
changing metadata and use `sqlalchemy <https://sqlalchemy.org>`_ to search the
database. But the models are only the Python classes that model the metadata. 
You will have to implement all steps to manage the data and check integrity yourself.

The command line interface offers some robust functionality to automate some 
common tasks and quickly add some entries. But the CLI gives you less freedom and 
some of the API and Model functionality is not implemented in the CLI.

The Python API gives you the best balance between abstraction and usability. Usually,
you will import the api and do all necessary work on the database from this entrypoint.

.. code-block:: python 

    from metacatalog import api
