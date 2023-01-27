"""
Metacatalog database revision
-----------------------------
date: 2023-01-27T17:10:15.164557

revision #14

Drop deprecated column entries.geom

"""
from sqlalchemy.orm import Session
from metacatalog import api, models

UPGRADE_SQL = """
-- drop column entries.geom
ALTER TABLE entries
DROP COLUMN geom;
COMMIT;
"""

DOWNGRADE_SQL = """
-- add column entry.geom
ALTER TABLE entries
ADD geom geometry(Geometry,4326);
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    print('run update')
    # drop column entries.geom
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # add column entries.geom
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)

