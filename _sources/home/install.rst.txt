============
Installation
============

Prerequisites
-------------

First you need to install PostgreSQL and the PostGIS extension.
You can find the PostgreSQL installer for Windows on the official PostgreSQL website.
Make sure that the stack installer is installed during the PostgreSQL installation process to install the PostGIS extension as well.

On Linux the commands might look similar to:

.. code-block:: bash

    sudo apt install postgresql postgis


PostGIS will in many cases be a rather outdated version. This is up to now not a big issue, as
metacatalog uses only a limited amount of spatial functions. Anything > v2.0 should be fine.

Next, you need to install the database and create the PostGIS extension. In this example, the chosen database name is 'metacatalog'.
You can create the database and the extension in the GUI application pgAdmin, which is installed together with PostgreSQL or
you can open a SQL console to postgresql or use psql:

.. code-block:: sql

    create database metacatalog with encoding='UTF8';
    create extension postgis;

Install metacatalog
-------------------

You can install metacatalog from PyPI

.. code-block:: bash

    pip install metacatalog


Create Tables
-------------

.. note::

    Refer to the CLI command `create <../cli/cli_create.ipynb>`_, `populate <../cli/cli_populate.ipynb>`_ and
    `init <../cli/cli_init.ipynb>`_ for more detailed information.

After the database has been installed, you can use the `metacatalog CLI <../cli/cli.rst>`_
to create the necessary tables.
Follow the syntax of the init command and replace *driver*, *user*, *password*, *host* and *database* with your parameters

.. note::

    All parameters can be accessed in pgAdmin or have been previously set by the user.

.. code-block:: bash

    metacatalog init --connection driver://user:password@host:port/database

When using Windows this command can lead to errors and must be changed in this case (refer to `metacatalog CLI <../cli/cli.rst>`_):

.. code-block:: bash

    python -m metacatalog init --connection driver://user:password@host:port/database




The (standard) connection command could look like this:

.. code-block:: bash

    metacatalog init --connection postgresql://postgres:\ *yourpassword*\ @localhost:5432/metacatalog


.. note::

    If you get a **FileNotFoundError** when first running the init command, try to (re)install the shapely package with **conda install shapely**.
