"""
Metacatalog database revision
-----------------------------
date: 2021-07-26T16:50:02.870032

revision #8

Change the unit of electrical conductivity.

"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL = """
-- replace columns (name, symbol) in row ('electrical conductivity', 'EC') with ('millisiemens per centimeter', 'mS/cm') in table units
UPDATE units
   SET name = 'millisiemens per centimeter',
       symbol = 'mS/cm'
WHERE name = 'electrical conductivity';
COMMIT;
"""

DOWNGRADE_SQL = """
-- replace columns (name, symbol) in row ('millisiemens per centimeter', 'mS/cm') with ('electrical conductivity', 'EC') in table units
UPDATE units
   SET name = 'electrical conductivity',
       symbol = 'EC'
WHERE name = 'millisiemens per centimeter';
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
