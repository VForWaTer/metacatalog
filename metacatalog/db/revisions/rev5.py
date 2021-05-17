"""
Metacatalog database revision
-----------------------------
date: 2021-04-15T15:56:28.128285

revision #5
~~~~~~~~~~~

Turns the Detail.value column into a JSONB type.


"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL = """
-- add the column
ALTER TABLE public.details ADD COLUMN raw_value JSONB;

-- update the values
UPDATE public.details SET raw_value=('{"__literal__": "' || value || '"}')::JSONB;

-- drop old column and make value not null
ALTER TABLE public.details DROP COLUMN value;
ALTER TABLE public.details ALTER COLUMN raw_value SET NOT NULL;
COMMIT;
"""

DOWNGRADE_SQL = """
-- add the old column back
ALTER TABLE public.details ADD COLUMN value character varying;

-- update the literal values
UPDATE public.details SET value=(raw_value->'__literal__')::character varying WHERE raw_value->'__literal__' IS NOT NULL;
UPDATE public.details SET value=raw_value::character varying WHERE raw_value->'__literal__' IS NULL;

-- drop the raw_values
ALTER TABLE public.details DROP COLUMN raw_value;
ALTER TABLE public.details ALTER COLUMN values SET NOT NULL;
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
