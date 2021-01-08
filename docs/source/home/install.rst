============
Installation
============

Prerequisites
-------------

First you need to install PostgreSQL and the PostGIS extension. There are preinstalled binaries
for windows.
On Linux the commands might look similar to:

.. code-block:: bash

    sudo apt install postgresql postgis


PostGIS will in many cases be a rather outdated version. This is up to now not a big issue, as
metacatalog uses only a limited amount of spatial functions. Anything > v2.0 should be fine.

Next, you need to install the database and create the extension. The database name should fit
the one specified in the connection string above (or change the string). You can open a SQL
console to postgresql or use psql:

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

After the database has been installed, you can use the `metacatalog CLI <../cli/cli.rst>`
to create the necessary tables:

.. code-block:: bash

    metacatalog init --connection postgresql://user:password@host:port/dbname
