"""rework dates

Revision ID: e2c9518375a0
Revises: 40607fb2d4e6
Create Date: 2020-06-11 06:59:46.286248

"""
from alembic import op
from sqlalchemy import Column, DateTime
from sqlalchemy.orm.session import Session
from metacatalog import api


# revision identifiers, used by Alembic.
revision = 'e2c9518375a0'
down_revision = '40607fb2d4e6'
branch_labels = None
depends_on = None

sql ="""UPDATE entrygroups SET "{column}"=(now() at time zone 'utc')::timestamp without time zone"""


def upgrade():
    # add the columns
    op.add_column('datasources', Column('creation', DateTime))
    op.add_column('datasources', Column('lastUpdate', DateTime))

    # fill last date
    op.execute(sql.format(column='lastUpdate'))

    # set creation from Entry.creation
    session = Session(op.get_bind())
    for e in api.find_entry(session):
        if e.datasource is not None:
            q = "UPDATE datasources SET creation='{c}' where id={id}"
            d = session.execute('select creation from entries where id=%d' % e.id).scalar()
            if d is not None:
                op.execute(q.format(c=d, id=e.datasource.id))
    
    # drop the columns in Entry
    op.drop_column('entries', 'creation')
    op.drop_column('entries', 'end')

def downgrade():
    # add back the deleted columns
    op.add_column('entries', Column('creation', DateTime))
    op.add_column('entries', Column('end', DateTime))

    # copy the creation back to Entry
    session = Session(op.get_bind())
    for e in api.find_entry(session):
        if e.datasource is not None:
            q = "UPDATE entries SET creation='{c}' where id={id}"
            d = session.execute('select creation from datasources where id=%d' % e.datasource.id).scalar()
            if d is not None:
                op.execute(q.format(c=d, id=e.id))

    # drop the date columns on datasource
    op.drop_column('datasources', 'creation')
    op.drop_column('datasources', 'lastUpdate')
