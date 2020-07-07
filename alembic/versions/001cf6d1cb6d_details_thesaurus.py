"""details_thesaurus

Revision ID: 001cf6d1cb6d
Revises: b2220bb497cb
Create Date: 2020-07-07 11:06:06.488381

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001cf6d1cb6d'
down_revision = 'b2220bb497cb'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('details', sa.Column('thesaurus_id', sa.Integer, sa.ForeignKey('thesaurus.id')))


def downgrade():
    op.drop_column('details', 'thesaurus_id')
