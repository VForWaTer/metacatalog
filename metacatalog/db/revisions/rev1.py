"""
Metacatalog database revision
-----------------------------
date: 2020-07-09T14:27:55.041635

Log Setup
Adds a log for existing database instances
"""
from sqlalchemy.orm import Session
from metacatalog import api, models


# define the upgrade function
def upgrade(session: Session):
    log = models.Log(
        code=models.LogCodes.info,
        description="Database was created in metacatalog < 0.2 version.",
        migration_head=1
    )
    session.add(log)


# define the downgrade function
def downgrade(session: Session):
    session.execute('DELETE from logs;')

