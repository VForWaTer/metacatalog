"""license rework

Revision ID: dce6a45a0786
Revises: e2c9518375a0
Create Date: 2020-06-11 14:54:11.316308

"""
from alembic import op
import sqlalchemy as sa

import pandas as pd 
from io import StringIO

# revision identifiers, used by Alembic.
revision = 'dce6a45a0786'
down_revision = 'a7544dad1e13'
branch_labels = None
depends_on = None


OLD_LICENSES="""id,short_title,title,summary,full_text,link,by_attribution,share_alike,commercial_use
1,ODbL,Open Data Commons Open Database License,This record and associated data sets are made available under the Open Database License: http://opendatacommons.org/licenses/odbl/1.0/. Any rights in individual contents of the database are licensed under the Database Contents License: http://opendatacommons.org/licenses/dbcl/1.0/,,https://opendatacommons.org/licenses/odbl/index.html,True,True,True
2,ODC-by,Open Data Commons Attribution License v1.0,This record and associated data sets are made available under the Open Data Commons Attribution License: http://opendatacommons.org/licenses/by/{version}.This {DATA(BASE)-NAME} is made available under the Open Data Commons Attribution License: http://opendatacommons.org/licenses/by/{version},,https://opendatacommons.org/licenses/by/index.html,True,False,True
3,PDDL,Open Data Commons Public Domain Dedication and License, This record and associated data sets are made available under the Public Domain Dedication and License v1.0 whose full text can be found at: http://www.opendatacommons.org/licenses/pddl/1.0/,,https://opendatacommons.org/licenses/pddl/index.html,False,False,True
"""

NEW_LICENSES = """id,short_title,title,summary,full_text,link,by_attribution,share_alike,commercial_use
4,ODbL,Open Data Commons Open Database License,"This record and associated data sets are made available under the Open Database License: http://opendatacommons.org/licenses/odbl/1.0/. Any rights in individual contents of the database are licensed under the Database Contents License: http://opendatacommons.org/licenses/dbcl/1.0/",,https://opendatacommons.org/files/2018/02/odbl-10.txt,True,True,True
5,ODC-by,Open Data Commons Attribution License v1.0,"This record and associated data sets are made available under the Open Data Commons Attribution License: http://opendatacommons.org/licenses/by/1.0.This data is made available under the Open Data Commons Attribution License: http://opendatacommons.org/licenses/by/1.0",,https://opendatacommons.org/files/2018/02/odc_by_1.0_public_text.txt,True,False,True
6,CC BY 4.0,Creative Commons Attribution 4.0 International,"You are free to: Share — copy and redistribute the material in any medium or format; Adapt — remix, transform, and build upon the material; for any purpose, even commercially. Under the following terms: Attribution — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.  ShareAlike — If you remix, transform, or build upon the material, you must distribute your contributions under the same license as the original.",,https://creativecommons.org/licenses/by/4.0/legalcode.txt,True,False,True
7,CC BY-SA 4.0,Creative Commons Attribution-ShareAlike 4.0 International,"You are free to: Share — copy and redistribute the material in any medium or format; Adapt — remix, transform, and build upon the material; for any purpose, even commercially. Under the following terms: Attribution — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.  ShareAlike — If you remix, transform, or build upon the material, you must distribute your contributions under the same license as the original.",,https://creativecommons.org/licenses/by-sa/4.0/legalcode.txt,True,True,True
8,CC BY-NC 4.0,Creative Commons Attribution-NonCommerical 4.0 International,"You are free to: Share — copy and redistribute the material in any medium or format Adapt — remix, transform, and build upon the material. Under the following terms: Attribution — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.  NonCommercial — You may not use the material for commercial purposes.",,https://creativecommons.org/licenses/by-nc/4.0/legalcode.txt ,True,False,False
9,CC BY-NC-SA 4.0,Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International,"You are free to: Share — copy and redistribute the material in any medium or format Adapt — remix, transform, and build upon the material. Under the following terms: Attribution — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.  NonCommercial — You may not use the material for commercial purposes.  ShareAlike — If you remix, transform, or build upon the material, you must distribute your contributions under the same license as the original.",,https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode.txt,True,True,False"""


def _load_to_db(txt):
    # create file-like object
    s = StringIO()
    s.write(txt)
    s.seek(0)
    csv: pd.DataFrame = pd.read_csv(s)
    csv.to_sql('licenses', op.get_bind(), if_exists='append', index=False)



def upgrade():
    # load new values
    _load_to_db(NEW_LICENSES)

    # update the foreign keys
    op.execute('UPDATE entries SET license_id=4 where license_id=1;')
    op.execute('UPDATE entries SET license_id=5 where license_id=2;')
    op.execute('UPDATE entries SET license_id=NULL where license_id=3;')

    # delete the old
    op.execute('DELETE FROM licenses where id in (1,2,3);')


def downgrade():
    # load the old values
    _load_to_db(OLD_LICENSES)

    # update the foreign keys
    op.execute('UPDATE entries SET license_id=1 where license_id=4;')
    op.execute('UPDATE entries SET license_id=2 where license_id=5;')

    # Delete the new licenses
    op.execute('DELETE FROM licenses where id in (4,5,6,7,8,9);')
    
