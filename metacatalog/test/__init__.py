r"""
Metacatalog Tests
-----------------

Metacatalog is not covered by *classic* unit tests.
We would have to write way too many, really detailed tests and 
rely on too much mocking.

Metacatalog rather implements some high-level e2e-tests. 
All of them can be executed with py.test _`https://docs.pytest.org/en/latest/index.html`

They are meant to be run by the Github actions script e2e.yml, which will
test against an actual dockerized PostgreSQL/PostGIS instance. 

If you want to run the tests locally you will need to install py.test:

.. code-block:: bash
  pip install pytest pytest-depends

Next, you need a Postgis database. Per default the tests will try to connect
to 

``host=localhost
"""