"""
Metacatalog database revision
-----------------------------
date: 2023-01-11T16:12:15.136872

revision #13

Update column keywords.thesaurus_id, which is [null] at
the moment. Currently, only GCMD thesaurus (id=1) is 
implemented, so column is updated with value 1

"""
from sqlalchemy.orm import Session
from metacatalog import api, models

UPGRADE_SQL = """
-- keywords.thesaurus_id = 1
UPDATE keywords SET thesaurus_id = 1;
COMMIT;
"""

DOWNGRADE_SQL = """
-- keywords.thesaurus_id = NULL
UPDATE keywords SET thesaurus_id = NULL;
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    print('run update')
    # keywords.thesaurus_id = 1
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # keywords.thesaurus_id = NULL
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)

