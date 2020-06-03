"""new variables added

Revision ID: 121df083eedc
Revises: 6ea73027b19b
Create Date: 2020-06-02 12:59:45.494130

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm.session import Session
from metacatalog import models


# revision identifiers, used by Alembic.
revision = '121df083eedc'
down_revision = '6ea73027b19b'
branch_labels = None
depends_on = None


def upgrade():
    # correct typo
    op.execute("UPDATE variables SET name='volumetric water content' WHERE id=12")

    # get a session
    session = Session(bind=op.get_bind())

    # add mega-pascal
    mpa = models.Unit(id=24, name='megapascal', symbol='MPa', si='10^6*kg*m^-1*s^-2')
    ec = models.Unit(id=25, name='electrical conductivity', symbol='EC', si='S*m^1 -1')
    session.add(mpa)
    session.add(ec)
    session.commit()

    # add variable
    mat = models.Variable(id=15, name='matric potential', symbol='phi', unit=mpa)
    bec = models.Variable(id=16,name='bulk electrical conductivity', symbol='bEC', unit=ec, keyword_id=5111)
    sec = models.Variable(id=17, name='specific electrical conductivity', symbol='sEC', unit=ec, keyword_id=5111)
    lev = models.Variable(id=18, name='river water level',symbol='L', unit_id=2)
    session.add(mat)
    session.add(bec)
    session.add(sec)
    session.add(lev)
    session.commit()

    


def downgrade():
    # add the typo
    op.execute("UPDATE variables SET name='volumnetric water content' WHERE id=12")
    
    # drop additional variables
    op.execute("DELETE FROM variables WHERE id in (15, 16, 17, 18)")
    
    # drop additional units
    op.execute("DELETE FROM units WHERE id in (24, 25)")

    
