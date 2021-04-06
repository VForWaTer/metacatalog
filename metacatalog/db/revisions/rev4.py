"""
Metacatalog database revision
-----------------------------
date: 2021-04-06T12:47:48.535397

revision #4

Add EntryGroup type 'Split dataset' for datasets that need to be split, e.g. when the temporal resolution of the data is changing.

"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL = """
-- add EntryGroup type 'Split dataset'
INSERT into entrygroup_types (name, description) values ('Split dataset', 'A split dataset groups a number of identical datasets that have to be split e.g. in case of different time scale resolution.');
COMMIT;
"""
DOWNGRADE_SQL = """
DELETE FROM entrygroup_types where name='Split dataset';
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
