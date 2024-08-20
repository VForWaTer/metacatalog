=========
E2E Tests
=========

Testing in metacatalog
======================

metacatalog includes an icreasing suite of tests to assure functional correctness.
As typical unittests would need some serious amount of database mocking and be 
of one magnitude larger than the database management code, it was decided to 
include end-to-end tests, as they are common in javascript frameworks.

The tests are implemented using pytest and can be run like that. They run 
common tasks like importing, editing and searching data and compare the 
results of queries against expected results.

On this path, some minor functions might not be covered by a unittest, but 
in a larger context, the tests assure that the whole application does what 
it should do.

.. note::

    metacatalog is a one-man project. If you spot a function that is not 
    covered and needs coverage, contact me via Github but be patient please.

Run 
===

Running tests locally
---------------------

The tests are designed to run on Github actions, by actually installing a 
PostgreSQL database and actually uploading data into the DB. You can run 
the tests locally, as well. You need to install:

.. code-block:: python

    pip install pytest pytest-cov pytest-depends

as these testing packages are not in the metacatalog requirements. 
By simply running:

.. code-block:: bash

    pytest 

metacatalog will install a new database instance called ``'test_[a-z]8`` 
with user ``postgres`` and password``postgres`` at port ``5432``. 
You can overwrite these settings by the environment variables:

* ``POSTGRES_USER``
* ``POSTGRES_PASSWORD``
* ``POSTGRES_PORT``

.. note::

    After each test run, a ``DBNAME`` file will be added to the 
    ``metacatalog/test`` folder, to track the current database 
    between the tests. After the tests have finished, you should 
    remove this file. This might be done by an optional cleanup 
    tests in the future.

Clean up
--------

If you run a lot of local tests, or if your copy of metacatalog runs 
tests after upgrading (which makes a lot of sense), you will find 
yourself left with maybe hundereds of databases, as the test suite does 
not drop the database. 

Run the following chunk to delete all databases that follow the 
naming convention of metacatalog tests:

.. code-block:: bash

    sudo -u postgres psql -d postgres < <( sudo -u postgres psql -Atc "select 'drop database \"' || datname || '\";' from pg_database where datname like 'test_%';")

.. warning::

    If you have other databases that start with ``test_``, **they will be deleted as well.**