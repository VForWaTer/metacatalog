"""datasource rework

Revision ID: 8d391ee1cdf1
Revises: d9b539aa0f56
Create Date: 2020-06-22 09:44:26.868275

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8d391ee1cdf1'
down_revision = 'd9b539aa0f56'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('datasources', sa.Column('encoding', sa.String(64)))

    op.execute("UPDATE datasources SET encoding='utf-8'")


def downgrade():
    op.drop_column('datasources', 'encoding')
