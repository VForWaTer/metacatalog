"""Initial commit

Revision ID: c459be723145
Revises: 
Create Date: 2020-05-15 20:06:05.889428

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c459be723145'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    print('---------------------------')
    print('This is the first revision.')
    print('\nInstallation and db model creation instructions can be found at')
    print('https://github.com/VForWaTer/metacatalog')
    print('\n----------------------------')
    print('Or try to run: ')
    print('python -m metacatalog init -C driver://user:password@host:port/dbname')
    


def downgrade():
    print('Cannot downgrade. This is the first database revision')
