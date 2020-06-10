"""add Entry.uuid

Revision ID: 88d7da3f7de6
Revises: 648bc46db836
Create Date: 2020-06-10 14:59:21.722641

"""
from uuid import uuid4
from alembic import op
from sqlalchemy import Column, String
from sqlalchemy.orm.session import Session
from metacatalog import api

# revision identifiers, used by Alembic.
revision = '88d7da3f7de6'
down_revision = '648bc46db836'
branch_labels = None
depends_on = None


def upgrade():
    # add the column
    op.add_column('entries', Column('uuid', String(36), nullable=True))

    # get a session
    session = Session(bind=op.get_bind())
    
    # load all entries
    for e in api.find_entry(session):
        # set UUID
        e.uuid = str(uuid4())
        session.add(e)
        session.commit()

    # set not null
    op.alter_column('entries', 'uuid', nullablee=False)


def downgrade():
    op.drop_column('entries', 'uuid')
