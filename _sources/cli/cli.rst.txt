===
CLI
===

.. toctree::
    :maxdepth: 1
    :Caption: Command Overview
    :hidden:

    cli_connection
    cli_create
    cli_populate
    cli_init
    cli_add
    cli_find
    cli_show
    cli_uuid

Command Line Interface
======================


The `setup.py` used on installing metacatalog installed a command line script 
which is also registered in the current anaconda environment (if used any). Therefore, just running 

.. code-block:: bash

    metacatalog


should work fine. 
If you however experience problems, which seem to happen on Windows quite frequently, 
there is also a CLI-like entrypoint into ``metacatalog``. You can use it like:

.. code-block:: powershell

    python -m metacatalog


Under the hood, exactly the same script gets executed.

Builtin Help
=============

Like with most cli, you can pass the ``-h`` flag to any command and sub-command to show
the builtin help for the current command.

.. code-block:: bash

    metacatalog -h

renders:
::

    usage: metacatalog [-h] {create,populate,init,connection,find,show,add} ...

    MetaCatalog management CLI

    optional arguments:
        -h, --help            show this help message and exit

    Commands:
        CLI commands

        {create,populate,init,connection,find,show,add}
        create              Create a new Metacatalog instance.
        populate            Populate the database with default auxiliary data.
        init                Runs the create and and the populate command.
        connection          Manage stored connections
        find                Find records in the database on exact matches.
        show                Show database structure or records.
        add                 Add new records to the database. Has to be combined
                            with one of the data origin flags.


Command Overview
================

.. nbgallery::
    :caption: Command Overview
    :name: cmd-gallery
    :glob: 
    :reversed: 

    cli_*