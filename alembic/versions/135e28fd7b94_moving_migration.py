"""moving migration

Revision ID: 135e28fd7b94
Revises: 61fbee10264f
Create Date: 2020-07-09 10:44:33.218734

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '135e28fd7b94'
down_revision = '61fbee10264f'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('logs', 
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('tstamp', sa.DateTime, nullable=False),
        sa.Column('code', sa.Integer, nullable=False),
        sa.Column('description', sa.String, nullable=False),
        sa.Column('migration_head', sa.Integer, nullable=True)
    )


def downgrade():
    op.drop_table('logs')
