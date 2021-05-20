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
ALTER TABLE datasources ADD COLUMN data_names CHARACTER VARYING(128)[];

-- add new variables
INSERT INTO variables (id,name,symbol,column_names,unit_id,keyword_id) VALUES
    (19,'evapotranspiration','ET','{"evapotranspiration"}',103,6319),
    (20,'drainage','D','{"drainage"}',103,7328)
ON CONFLICT ON CONSTRAINT variables_pkey 
DO 
    UPDATE SET column_names=EXCLUDED.column_names;

-- add column names
UPDATE variables set column_names='{"air_temperature"}' WHERE id=1;
UPDATE variables set column_names='{"soil_temperature"}' WHERE id=2;
UPDATE variables set column_names='{"water_temperature"}' WHERE id=3;
UPDATE variables set column_names='{"discharge"}' WHERE id=4;
UPDATE variables set column_names='{"air_pressure"}' WHERE id=5;
UPDATE variables set column_names='{"relative_humidity"}' WHERE id=6;
UPDATE variables set column_names='{"daily_rainfall_sum"}' WHERE id=7;
UPDATE variables set column_names='{"rainfall_intensity"}' WHERE id=8;
UPDATE variables set column_names='{"solar_irradiance"}' WHERE id=9;
UPDATE variables set column_names='{"net_radiation"}' WHERE id=10;
UPDATE variables set column_names='{"gravimetric_water_content"}' WHERE id=11;
UPDATE variables set column_names='{"volumetric_water_content"}' WHERE id=12;
UPDATE variables set column_names='{"precision"}' WHERE id=13;
UPDATE variables set column_names='{"sap_flow"}' WHERE id=14;
UPDATE variables set column_names='{"matric_potential"}' WHERE id=15;
UPDATE variables set column_names='{"bulk_electrical_conductivity"}' WHERE id=16;
UPDATE variables set column_names='{"specific_electrical_conductivity"}' WHERE id=17;
UPDATE variables set column_names='{"river_water_level"}' WHERE id=18;

-- column names are build therefore the data_names can be filled
UPDATE datasources SET data_names=column_names
FROM entries JOIN variables ON entries.variable_id=variables.id
WHERE datasources.id = entries.datasource_id;

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

-- make entrygroup titles longer
ALTER TABLE entrygroups ALTER COLUMN title TYPE character varying(250);

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
ALTER TABLE datasources DROP COLUMN data_names;

-- delete timeseries
DROP TABLE timeseries;
COMMIT;

-- rename the stuff back
ALTER TABLE timeseries_1d RENAME TO timeseries;
ALTER TABLE timeseries RENAME CONSTRAINT timeseries_1d_pkey TO timeseries_pkey;
ALTER TABLE timeseries RENAME CONSTRAINT timeseries_1d_entry_id_fkey TO timeseries_entry_id_fkey;

-- update datasources
UPDATE datasources SET path='timeseries' WHERE path='timeseries_1d';

-- change entrygroup title back
ALTER TABLE entrygroups ALTER COLUMN title TYPE character varying(40);
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