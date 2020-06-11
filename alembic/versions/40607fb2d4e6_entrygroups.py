"""EntryGroups

Revision ID: 40607fb2d4e6
Revises: 88d7da3f7de6
Create Date: 2020-06-10 19:35:17.787370

"""
from alembic import op
from sqlalchemy import Column, DateTime

# revision identifiers, used by Alembic.
revision = '40607fb2d4e6'
down_revision = '88d7da3f7de6'
branch_labels = None
depends_on = None

sql ="""UPDATE entrygroups SET "{column}"=(now() at time zone 'utc')::timestamp without time zone"""

def upgrade():
    # add missing datetime columns
    op.add_column('entrygroups', Column('publication', DateTime))
    op.add_column('entrygroups', Column('lastUpdate', DateTime))

    # update the fields
    op.execute(sql.format(column='publication'))
    op.execute(sql.format(column='lastUpdate'))

def downgrade():
    op.drop_column('entrygroups', 'publication')
    op.drop_column('entrygroups', 'lastUpdate')
