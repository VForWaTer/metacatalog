"""person rework

Revision ID: a7544dad1e13
Revises: e2c9518375a0
Create Date: 2020-06-13 12:50:37.248618

"""
from alembic import op
from sqlalchemy import Column, String


# revision identifiers, used by Alembic.
revision = 'a7544dad1e13'
down_revision = 'e2c9518375a0'
branch_labels = None
depends_on = None


def upgrade():
    # make first_name NOT NULL
    op.alter_column('persons', 'first_name', nullable=False)

    # add new columns
    op.add_column('persons', Column('organisation_name', String(1024)))
    op.add_column('persons', Column('attribution', String(1024)))


def downgrade():
    # make first name optional again
    op.alter_column('persons', 'first_name', nullable=True)

    # drop the new columns
    op.drop_column('persons', 'organisation_name')
    op.drop_column('persons', 'attribution')
