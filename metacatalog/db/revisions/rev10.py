"""
Metacatalog database revision
-----------------------------
date: 2022-10-18T11:22:31.732508

revision #10

Add the datasource_type "netCDF"


"""
from sqlalchemy.orm import Session
from metacatalog import api, models

UPGRADE_SQL = """
-- add new datasource_type ("netCDF")
INSERT INTO datasource_types (id, name, title, description) VALUES 
(5, 'netCDF','Local netCDF File','netCDF file source on the local file system.');
COMMIT;
"""

DOWNGRADE_SQL = """
-- delete new datasource_type ("netCDF")
DELETE FROM datasource_types WHERE name='netCDF';
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session): 
    print('run update')
    # create the datasource_type
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # delete the new datasource_type
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)

