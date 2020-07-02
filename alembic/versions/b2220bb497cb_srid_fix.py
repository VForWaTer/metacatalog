"""SRID fix

Revision ID: b2220bb497cb
Revises: 12db7faa976f
Create Date: 2020-06-26 11:01:47.331106

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b2220bb497cb'
down_revision = '12db7faa976f'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE entries SET location=ST_SetSRID(location, 4326);")
    op.execute("ALTER TABLE entries ALTER COLUMN location TYPE geometry('POINT', 4326);")
    


def downgrade():
    op.execute("ALTER TABLE entries ALTER COLUMN location TYPE geometry")
    op.execute("UPDATE entries SET location=ST_SetSRID(location, 0);")
