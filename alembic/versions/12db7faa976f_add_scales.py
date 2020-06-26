"""add scales

Revision ID: 12db7faa976f
Revises: 8d391ee1cdf1
Create Date: 2020-06-26 10:42:30.751709

"""
from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry


# revision identifiers, used by Alembic.
revision = '12db7faa976f'
down_revision = '8d391ee1cdf1'
branch_labels = None
depends_on = None


def upgrade():
    # ADD temporal scales
    op.create_table('temporal_scales',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('resolution', sa.String, nullable=False),
        sa.Column('observation_start', sa.DateTime, nullable=False),
        sa.Column('observation_end', sa.DateTime, nullable=False),
        sa.Column('support', sa.Numeric, sa.CheckConstraint('support >= 0'), nullable=False)
    )

    # ADD spatial scale
    op.create_table('spatial_scales',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('resolution', sa.Integer, nullable=False),
        sa.Column('extent', Geometry(geometry_type='POLYGON', srid=4326), nullable=False),
        sa.Column('support', sa.Numeric, sa.CheckConstraint('support >= 0'), nullable=False)
    )

    # create foreign keys
    op.add_column('datasources', 
        sa.Column('temporal_scale_id', sa.Integer, sa.ForeignKey('temporal_scales.id'))
    )
    op.add_column('datasources',
        sa.Column('spatial_scale_id', sa.Integer, sa.ForeignKey('spatial_scales.id'))
    )


def downgrade():
    # drop foreign keys
    op.drop_column('datasources', 'temporal_scale_id')
    op.drop_column('datasources', 'spatial_scale_id')

    # drop tables
    op.drop_table('temporal_scales')
    op.drop_table('spatial_scales')
