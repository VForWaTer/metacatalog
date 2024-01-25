"""
Metacatalog database revision
-----------------------------
date: 2024-01-24T11:27:59.289549

revision #15

Add columns datasources.variable_names, temporal_scales.dimension_names
and spatial_scales.dimension_names of type ARRAY(String) to store
variable and dimension names.

"""
from sqlalchemy.orm import Session
from metacatalog import api, models


UPGRADE_SQL = """
-- datasources.variable_names VARCHAR ARRAY 128 letters limit
ALTER TABLE datasources
ADD COLUMN variable_names VARCHAR(128)[];
COMMIT;
-- temporal_scales.dimension_names VARCHAR ARRAY 128 letters limit
ALTER TABLE temporal_scales
ADD COLUMN dimension_names VARCHAR(128)[];
COMMIT;
-- spatial_scales.dimension_names VARCHAR ARRAY 128 letters limit
ALTER TABLE spatial_scales
ADD COLUMN dimension_names VARCHAR(128)[];
COMMIT;
"""

DOWNGRADE_SQL = """
-- drop column datasources.variable_names
ALTER TABLE datasources
DROP COLUMN variable_names;
COMMIT;
-- drop column temporal_scales.dimension_names
ALTER TABLE temporal_scales
DROP COLUMN dimension_names;
COMMIT;
-- drop column spatial_scales.dimension_names
ALTER TABLE spatial_scales
DROP COLUMN dimension_names;
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    print('run update')
    # add columns datasources.variable_names, temporal_scales.dimension_names and spatial_scales.dimension_names
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # drop columns datasources.variable_names, temporal_scales.dimension_names and spatial_scales.dimension_names
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)
