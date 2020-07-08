"""
Metacatalog database revision
-----------------------------
date: 2020-07-08T20:55:45.531617

revision #0

"""
from sqlalchemy.orm import Session
from metacatalog import api, models


# define the upgrade function
def upgrade(session: Session):
    print('run update')


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')

