"""
Metacatalog database revision
-----------------------------
date: 2020-07-08T20:55:45.531617

revision #0

"""
from sqlalchemy.orm import Session
from metacatalog import api, models

DWN_MSG = """This is the first revision.

With metacatalog==0.2 the migration system was rebuild.
To downgrade to metacatalog<0.2, use alembic as 
migration system.
"""

UP_MSG = """This is the first revision.

This revision does nothing more than printing this message.
Thank you and have a nice day!
"""

# define the upgrade function
def upgrade(session: Session):
    print(UP_MSG)


# define the downgrade function
def downgrade(session: Session):
    print(DWN_MSG)

