"""
Metacatalog database revision
-----------------------------
date: 2021-05-20T11:02:13.319954

revision #6


"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL = """
-- add a new Entrygroup type
INSERT INTO entrygroup_types (id, name, description) VALUES 
(4,'Label','A Label groups different datasets into a larger collection of datasets, that are now a composite, but i.e. collected at the same site.');

-- todo, here the new column creation is missing
ALTER TABLE variables ADD COLUMN column_names CHARACTER VARYING(128)[];

-- add new variables
INSERT INTO variables (id,name,symbol,column_names,unit_id,keyword_id) VALUES
    (19,'evapotranspiration','ET','{"evapotranspiration"}',103,6319),
    (20,'drainage','D','{"drainage"}',103,7328);

-- rename timeseries to timeseries_1d
ALTER TABLE timeseries RENAME TO timeseries_1d;
ALTER TABLE timeseries_1d RENAME CONSTRAINT timeseries_pkey TO timeseries_1d_pkey;
ALTER TABLE timeseries_1d RENAME CONSTRAINT timeseries_entry_id_fkey TO timeseries_1d_entry_id_fkey;

-- update datasources
UPDATE datasources SET path='timeseries_1d' WHERE path='timeseries';

-- create new table
CREATE TABLE timeseries (
    entry_id INTEGER NOT NULL,
    tstamp timestamp without time zone NOT NULL,
    "data" REAL[],
    "precision" REAL[]
);
ALTER TABLE timeseries ADD CONSTRAINT timeseries_pkey PRIMARY KEY (entry_id, tstamp);
ALTER TABLE timeseries ADD CONSTRAINT timeseries_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entries (id);

COMMIT;
"""

DOWNGRADE_SQL = """
-- delete entrygroups that use the Label type
DELETE FROM nm_entrygroups WHERE group_id in (SELECT id FROM entrygroups WHERE type_id=4);
DELETE FROM entrygroups WHERE type_id=4;

-- remove the entrygroup type
DELETE FROM entrygroup_types WHERE id=4;

-- remove the colmap column
ALTER TABLE variables DROP COLUMN column_names;
DELETE FROM VARIABLES WHERE id in (19, 20);

-- delete timeseries
DROP TABLE timeseries;
COMMIT;

-- rename the stuff back
ALTER TABLE timeseries_1d RENAME TO timeseries;
ALTER TABLE timeseries RENAME CONSTRAINT timeseries_1d_pkey TO timeseries_pkey;
ALTER TABLE timeseries RENAME CONSTRAINT timeseries_1d_entry_id_fkey TO timeseries_entry_id_fkey;

-- update datasources
UPDATE datasources SET path='timeseries' WHERE path='timeseries_1d';
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