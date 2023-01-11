"""
Metacatalog database revision
-----------------------------
date: 2022-12-16T17:43:29.087342

revision #12

Details adjustments:
- Details.key has now a limit of 60 letters (instead of 20)
- new column Details.title

"""
from sqlalchemy.orm import Session
from metacatalog import api, models

UPGRADE_SQL = """
-- details.key 60 letters limit
ALTER TABLE details ALTER COLUMN key TYPE varchar(60);
COMMIT;
-- create column details.title
ALTER TABLE details ADD COLUMN title VARCHAR;
COMMIT;
"""

DOWNGRADE_SQL = """
-- details.key 20 letters limit, shorten key if longer than 20 letters
UPDATE details SET key=LEFT(key, 20);
ALTER TABLE details ALTER COLUMN key TYPE varchar(20);
COMMIT;
-- delete column details.title
ALTER TABLE details DROP COLUMN title;
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    print('run update')
    # details key letter limit 60 and new column title
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # details key letter limit 20 and drop column title
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)
