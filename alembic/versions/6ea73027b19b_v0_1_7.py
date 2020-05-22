"""v0.1.7

Revision ID: 6ea73027b19b
Revises: c459be723145
Create Date: 2020-05-19 08:29:49.920644

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6ea73027b19b'
down_revision = 'c459be723145'
branch_labels = None
depends_on = None


def upgrade():
    # rename table
    op.rename_table('geneic_geometry_data', 'generic_geometry_data')


def downgrade():
    op.rename_table('generic_geometry_data', 'geneic_geometry_data')

