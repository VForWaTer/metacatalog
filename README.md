# metacatalog

[![GitHub version](https://badge.fury.io/gh/VForWaTer%2Fmetacatalog.svg)](https://badge.fury.io/gh/VForWaTer%2Fmetacatalog)
[![PyPI version](https://badge.fury.io/py/metacatalog.svg)](https://pypi.org/project/metacatalog/)
[![Dev status](https://img.shields.io/badge/development%20status-2%20--%20Alpha-yellow)](https://pypi.org/classifiers/)
![e2e Test](https://github.com/VForWaTer/metacatalog/workflows/e2e%20Test/badge.svg)
![Documentation](https://github.com/VForWaTer/metacatalog/workflows/Documentation/badge.svg)

Management tool for the V-FOR-WaTer metadata database application. Although developed in and for the [V-FOR-WaTer project](https://vforwater.de), metacatalog is a standalone database application, that can be used on its own.

## Docs

The full documentation is available at: https://vforwater.github.io/metacatalog

Installation description is given at: https://vforwater.github.io/metacatalog/home/install.html

## Quickstart

Install metacatalog:

```bash
pip install metacatalog
```

With a Postgis database called `metacatalog` installed at `localhost:5432` you can store a default connection like:
**Be aware that any password saved along with the default connection is saved in clear-text!!**

```bash
metacatalog connection --save postgresql://postgres:<masterpassword>@localhost:5432/metacatalog
```

Refer to the [connection documentation](https://vforwater.github.io/metacatalog/cli/cli_connection.html) to learn about all possibilities to connect to a database.

The database table can be installed and populated like:

```bash
metacatalog init -C postgresql://postgres:<password>@localhost:5432/metacatalog
```

And now you can use the database via:

* the [CLI](https://vforwater.github.io/metacatalog/cli/cli.html)
* the [Python API](https://vforwater.github.io/metacatalog/api/api.html)
* or directly interface to the [database model classes](https://vforwater.github.io/metacatalog/models/models.html)

## Using metacatalog on Windows

On one of my Windows systems the setuptools scripts does not get recognized 
properly and thus the CLI does not work properly if not called by full path.
Therefore with version 0.1.4 the CLI is implemented the module main entrypoint.
**Wherever the docs call the metacatalog script, you can use the module, like:**

Instead of:
```bash
metacatalog [options] <commad>
```
you can use:
```bash
python -m metacatalog [options] <command>
```
This should work cross-platform. Tested on Ubuntu 18, debian 9, Windows 7 and 
Windows 10. 
