"""
Metacatalog database revision
-----------------------------
date: 2020-11-27T13:43:00.463177

Persons
Extend person model to store organizations as well
"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL="""
-- add columns
ALTER TABLE public.persons ADD COLUMN is_organisation boolean;
ALTER TABLE public.persons ADD COLUMN organisation_abbrev character varying (64);

-- update existing
UPDATE public.persons SET is_organisation=false;
ALTER TABLE public.persons ALTER COLUMN is_organisation SET NOT NULL;

-- set constraints
ALTER TABLE public.persons ALTER COLUMN first_name DROP NOT NULL;
ALTER TABLE public.persons ALTER COLUMN last_name DROP NOT NULL;
ALTER TABLE public.persons ADD CONSTRAINT persons_check CHECK ( NOT (last_name IS NULL AND organisation_name IS NULL));
COMMIT;
"""
DOWNGRADE_SQL="""
-- remove organisations
ALTER TABLE public.persons DROP CONSTRAINT persons_check;
ALTER TABLE public.persons ALTER COLUMN first_name SET NOT NULL;
ALTER TABLE public.persons ALTER COLUMN last_name SET NOT NULL;
ALTER TABLE public.persons DROP COLUMN is_organisation;
ALTER TABLE public.persons DROP COLUMN organisation_abbrev;
COMMIT;
"""


# define the upgrade function
def upgrade(session: Session):
    # add the missing columns:
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    # go back
    try:
        with session.bind.connect() as con:
            con.execute(DOWNGRADE_SQL)

        print('Run:\n%s' % DOWNGRADE_SQL)
    except Exception as e:
        session.rollback()
        print('ERROR: there might still be organisations in the database. Remove them first.\n%s' % str(e))
