"""add is_partial to Entry

Revision ID: 648bc46db836
Revises: 70fbbdebf581
Create Date: 2020-06-05 19:14:54.924092

"""
from alembic import op
from sqlalchemy import Column, Boolean


# revision identifiers, used by Alembic.
revision = '648bc46db836'
down_revision = '70fbbdebf581'
branch_labels = None
depends_on = None


def upgrade():
    # add the column
    op.add_column('entries', Column('is_partial', Boolean, nullable=True, default=False))

    # set the default
    op.execute('UPDATE entries SET is_partial=False')

    # set column not null
    op.alter_column('entries', 'is_partial', nullable=False)

def downgrade():
    op.drop_column('entries', 'is_partial')
