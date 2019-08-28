# metacatalog

Management tool for the V-FOR-WaTer metadata database application.

## Install

There are two different use-cases. Either you want to connect to an 
existing instance of the database, or you need to install the database itself as well.
In both cases you need to install the python package.

A) Install from PyPI

```bash
pip install metacatalog
```

B) Install from Github

Right now, the version on Github is newer, as the package is under heavy development. 
Therefore, until a stable version 1.0 is released, it is recommended to install the package via 
Github.

```bash
git clone https://github.com/vforwater/metacatalog.git
cd metacatalog
python setup.py install
```

In either case, you can use the cli to save a default connection into a file in your home folder.
**Be aware that any password saved along with the default connection is saved in clear-text!!**

```bash
metacatalog connection --save postgresql://postgres:<masterpassword>@localhost:5432/metacatalog
```

In may be not the best idea to save the postgres user password in cleartext. You can of course
create a new user with limited rights and save this information. Alternatively, the connection can 
be specified on each call of the cli. This way there is no password saving.

Now, you are ready to use the cli. In case you need to install the database itself as well, follow the 
instructions below:

## Install the database

First you need to install PostgreSQL and the PostGIS extension. There are preinstalled binaries 
for windows. 
On Linux the commands might look similar to:

```bash
sudo apt install postgresql postgis
```

PostGIS will in many cases be a rather outdated version. This is up to now not a big issue, as 
metacatalog uses only a limited amount of spatial functions. Anything > v2.0 should be fine.

Next, you need to install the database and create the extension. The database name should fit 
the one specified in the connection string above (or change the string). You can open a SQL
console to postgresql or use psql:

```SQL
create database metacatalog with encoding='UTF8';
create extension postgis;
```

Now, you are ready to go and let the CLI create all neccessary tables.

## Create Tables and load Defaults

The `create`command can be used to create all needed tables in the connected database.
You can specify the connection string using the `--connection` flag. If not supplied, the
CLI will search for a saved 'default' connection string. In case there is none, the CLI will 
error.

```bash
metacatalog create --connection postgresql://postgres:<password>@localhost:5432/metacatalog
```

will output:

```
Creating Tables.
Done.
```

The next step is to populate the tables with some useful default data. 
This step is optional, but recommended. As of this writing, the `populate`
command will load records into the `units`, `variables`, `licenses` and `keywords`.

```bash
metacatalog populate
```

will output:

```bash
Populating datasource_types
Finished datasource_types
Populating units
Finished units
Populating variables
Finished variables
Populating licenses
Finished licenses
Populating person_roles
Finished person_roles
Populating keywords
Finished keywords
```

The `--ignore` flag can be used to omit one or many tables from population.
Instead of using `create` and `populate`, the `init` will run both in only 
one step. The `init` command will accept the same flags.
Creating a new instance in a test database can be initialized like:

```bash
metacatalog init -C postgresql://postgres:<password>@localhost:5432/test --ignore units variables
```

This will create the same structure and data in the `test` database. The `variables` and `units` table will, however, be empty.

## Find data

Before storing your actual metadata into the database, it makes sense to 
learn how auxiliary information can be found in the database. The CLI exposes an `find` command to find records on exact matches. In a future release, a `search` endpoint will be added as well. 

Note: As of this writing the `find` command can not operate on all tables.

We can find all stored licenses by `find`ing them without any filter:

```bash
metacatalog find licenses
```
```
Open Data Commons Open Database License <ID=1>
Open Data Commons Attribution License v1.0 <ID=2>
Open Data Commons Public Domain Dedication and License <ID=3>
```

A filter can be added using the `--by` flag. This flag expects two values, the column to match and the actual value. We can use this to find all keywords that include `SOIL TEMPERATURE` on any level. The keywords are 
implemented self-referential and hold the keyword name in an attribute called `value`.

```bash
metacatalog find keywords --by value "SOIL TEMPERATURE"
```
```
EARTH SCIENCE > AGRICULTURE > SOILS > SOIL TEMPERATURE
EARTH SCIENCE > CLIMATE INDICATORS > LAND SURFACE/AGRICULTURE INDICATORS > SOIL TEMPERATURE
EARTH SCIENCE > CRYOSPHERE > FROZEN GROUND > SOIL TEMPERATURE
EARTH SCIENCE > LAND SURFACE > FROZEN GROUND > SOIL TEMPERATURE
EARTH SCIENCE > LAND SURFACE > SOILS > SOIL TEMPERATURE
```

Any of these keywords might be suitable to append them to your metadata to make your soil temperature data set findable on that keyword.