"""datasource rework

Revision ID: 8d391ee1cdf1
Revises: d9b539aa0f56
Create Date: 2020-06-22 09:44:26.868275

"""
from alembic import op
import sqlalchemy as sa

from io import StringIO
import pandas as pd

# revision identifiers, used by Alembic.
revision = '8d391ee1cdf1'
down_revision = 'd9b539aa0f56'
branch_labels = None
depends_on = None

DATATYPES = """id,parent_id,name,title,description
1,,blob,File Blob,"Any kind of file-like-structure. Reader will only return file paths. Not for use in production. Can be used as a placeholder for custom data management."
11,,array,generic array structure,"Arrays are a series of data objects, without index."
12,11,iarray,indexed array,"Array with additional index information for each element, that is not a datetime."
13,12,varray,"named, indexed array","An iarray that additionally has a name property of any valid metacatalog Variable."
14,11,timeseries,timeseries,"Array indexed by datetime information. The datetimes need to be of increasing order."
15,14,vtimeseries,named timeseries,"Timeseries that holds an additional Variable name to describe the content."
16,,ndarray,generic mulidimensional array,"NDArrays are multidimensional arrays of common atomic data-type."
17,16,raster,Raster data,"GDAL conform raster images."
18,17,vraster,named raster data,"The named raster images are not implemented yet."
19,16,2darray,2D-array,"Special case of NDArray with exactly two dimensions."
20,16,idataframe,indexed table,"NDArray with any index except datetime information."
21,20,vdataframe,"named, indexed table","idataframe with additional name property of any valid metacatalog Variable."
22,16,time-dataframe,timeseries table,"NDArray indexed by datetime information. The datetimes need to be of increasing order."
23,22,vtime-dataframe,named timeseries table,"Timeseries table that holds an additional Variable name to describe the content." """

def _load_to_db(txt):
    # create file-like object
    s = StringIO()
    s.write(txt)
    s.seek(0)
    csv: pd.DataFrame = pd.read_csv(s)
    csv.to_sql('datatypes', op.get_bind(), if_exists='append', index=False)


def upgrade():
    # ENCODING
    # add encoding column
    op.add_column('datasources', sa.Column('encoding', sa.String(64)))

    op.execute("UPDATE datasources SET encoding='utf-8'")

    # add datatype table
    dt = op.create_table('datatypes',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('parent_id', sa.Integer, sa.ForeignKey('datatypes.id')),
        sa.Column('name', sa.String(64), nullable=False),
        sa.Column('title', sa.String, nullable=False),
        sa.Column('description', sa.String, nullable=True)
    )

    # fill defaults
    _load_to_db(DATATYPES)
    op.execute("SELECT setval('datatypes_id_seq', 1000, true);")

    # add foreign key
    op.add_column('datasources', sa.Column('datatype_id', sa.Integer, sa.ForeignKey('datatypes.id'), nullable=True))
    op.execute("UPDATE datasources SET datatype_id=1")
    op.alter_column('datasources', 'datatype_id', nullable=False)




def downgrade():
    # drop new columns
    op.drop_column('datasources', 'encoding')
    op.drop_column('datasources', 'datatype_id')

    # drop new tables
    op.drop_table('datatypes')
