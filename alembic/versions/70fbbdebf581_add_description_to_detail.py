"""add description to detail

Revision ID: 70fbbdebf581
Revises: 121df083eedc
Create Date: 2020-06-03 12:40:17.551595

"""
from alembic import op
from sqlalchemy import Column, String


# revision identifiers, used by Alembic.
revision = '70fbbdebf581'
down_revision = '121df083eedc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('details', Column('description', String, nullable=True))


def downgrade():
    op.drop_column('details', 'description')
