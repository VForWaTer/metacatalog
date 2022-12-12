"""
Metacatalog database revision
-----------------------------
date: 2022-10-05T14:41:32.055433

revision #11

Make column entries.location nullable (raster data do not have a POINT location).
Attention, the downgrade sets everywhere where entries.location == NULL to 
(POINT 0 0), which may not correspond to the state of the database before upgrading,
so the revision upgrade cannot be undone. 

"""
from sqlalchemy.orm import Session
from metacatalog import api, models

UPGRADE_SQL = """
-- entries.location nullable
ALTER TABLE entries ALTER COLUMN location DROP NOT NULL;
COMMIT;
"""

DOWNGRADE_SQL = """
-- replace eventually existing NULL values with (POINT 0 0)
UPDATE entries SET location = 'SRID=4326; POINT (0 0)' WHERE location IS NULL;
COMMIT; 
-- entries.location not nullable
ALTER TABLE entries ALTER COLUMN location SET NOT NULL;
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    print('run update')
    # make entries.location nullable
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # make entries.location not nullable
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)

