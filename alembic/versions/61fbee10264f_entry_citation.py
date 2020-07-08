"""entry citation

Revision ID: 61fbee10264f
Revises: b2220bb497cb
Create Date: 2020-07-08 12:28:08.706800

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '61fbee10264f'
down_revision = 'b2220bb497cb'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('entries', sa.Column('citation', sa.String(2048), nullable=True))


def downgrade():
    op.drop_column('entries', 'citation')
