"""
Metacatalog database revision
-----------------------------
date: 2022-10-04T10:51:17.708333

revision #9

Add the license "dl-by-de/2.0"

"""
from sqlalchemy.orm import Session
from metacatalog import api, models

UPGRADE_SQL = """
-- add new license ("dl-by-de/2.0")
INSERT INTO licenses (id, short_title, title, summary, link, by_attribution, share_alike, commercial_use) VALUES 
(10, 'dl-by-de/2.0','Data licence Germany – attribution – version 2.0','Data and metadata may be (commercially and non-commercially) copied, printed, presented, altered, processed and transmitted to third parties; be merged with own and third party data; be integrated into internal and external business processes, products and applications if the user ensures that the source reference includes the name of the provider, the reference to this license and a reference to the dataset (URI) and that changes, editing, new designs or other amendments are marked in the source references.','http://www.govdata.de/dl-de/by-2-0',True,True,True);
COMMIT;
"""

DOWNGRADE_SQL = """
-- delete new license ("dl-by-de/2.0")
DELETE FROM licenses WHERE short_title='dl-by-de/2.0';
COMMIT;
"""

# define the upgrade function
def upgrade(session: Session):
    print('run update')
    # create the new license
    with session.bind.connect() as con:
        con.execute(UPGRADE_SQL)


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')
    # delete the new license
    with session.bind.connect() as con:
        con.execute(DOWNGRADE_SQL)

