"""
Metacatalog database revision
-----------------------------
date: 2021-06-07T08:57:42.792745

revision #7

Updates to the logging table.
The log codes are changed to be aligned to the builtin
logging module

"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL = """
-- update new codes
UPDATE logs SET code=40 WHERE code=99;
UPDATE logs SET code=30 WHERE code=10;
UPDATE logs SET code=20 WHERE code=5;

-- add the new column
ALTER TABLE logs ADD COLUMN code_name CHARACTER VARYING(20);

-- update names
UPDATE logs SET code_name='MIGRATION' WHERE code=1;
UPDATE logs SET code_name='DEBUG' WHERE code=10;
UPDATE logs SET code_name='INFO' WHERE code=20;
UPDATE logs SET code_name='WARNING' WHERE code=30;
UPDATE logs SET code_name='ERROR' WHERE code=40;

-- make the column not null
ALTER TABLE logs ALTER COLUMN code_name SET NOT NULL;
COMMIT;
"""

DOWNGRADE_SQL = """
-- update new codes
UPDATE logs SET code=99 WHERE code=40;
UPDATE logs SET code=5 WHERE code in (20,10);
UPDATE logs SET code=10 WHERE code=30;

-- drop code names
ALTER TABLE logs DROP COLUMN code_name;
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    # create the new EntryGroup type
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)
