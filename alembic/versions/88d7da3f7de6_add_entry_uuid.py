"""add Entry.uuid

Revision ID: 88d7da3f7de6
Revises: 648bc46db836
Create Date: 2020-06-10 14:59:21.722641

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '88d7da3f7de6'
down_revision = '648bc46db836'
branch_labels = None
depends_on = None


def upgrade():
    # add the column
    op.add_column('entries', Column(String(36), nullable=True))

    # fill for existing entries
    op.execute('UPDATE entries SET uuid=uuid_generate_v4()')

    # set not null
    op.alter_column('entries', 'uuid', nullablee=False)


def downgrade():
    op.drop_column('entries', 'uuid')
