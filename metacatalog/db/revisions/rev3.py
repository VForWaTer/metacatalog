"""
Metacatalog database revision
-----------------------------
date: 2021-02-05T08:48:28.689614

revision #3

Extend person to have a UUID

"""
from sqlalchemy.orm import Session
from metacatalog import api, models
from uuid import uuid4


UPGRADE_SQL = """
-- add columns
ALTER TABLE public.persons ADD COLUMN uuid character varying (36);
COMMIT;
"""
DOWNGRADE_SQL = """
ALTER TABLE public.persons DROP COLUMN uuid;
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    # create the new column
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)
    
    # fill any uuid that is mission
    persons = session.query(models.Person).filter(models.Person.uuid == None).all()
    for person in persons:
        person.uuid = str(uuid4())
        try:
            session.add(person)
            session.commit()
        except Exception as e:
            print('[ERROR]: %s' % str(e))


# define the downgrade function
def downgrade(session: Session):
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)

