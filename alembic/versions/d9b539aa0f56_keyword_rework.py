"""keyword rework

Revision ID: d9b539aa0f56
Revises: dce6a45a0786
Create Date: 2020-06-17 09:42:09.919978

"""
from alembic import op
from sqlalchemy import Column, String, Integer, ForeignKey


# revision identifiers, used by Alembic.
revision = 'd9b539aa0f56'
down_revision = 'dce6a45a0786'
branch_labels = None
depends_on = None


def upgrade():
    # remove the alias and values fields from nm_keywords_entries
    op.drop_column('nm_keywords_entries', 'alias')
    op.drop_column('nm_keywords_entries', 'associated_value')

    # add the thesaurus table
    the = op.create_table('thesaurus',
        Column('id', Integer, primary_key=True),
        Column('uuid', String(64), unique=True, nullable=False),
        Column('name',String(1024), nullable=False),
        Column('organisation', String, nullable=False),
        Column('url', String, nullable=False)
    )

    # add GCMD
    op.bulk_insert(the,[
        {
            "id": 1, 
            "uuid": "2e54668d-8fae-429f-a511-efe529420b12",
            "name": "GCMD",
            "organisation": "NASA",
            "description": "NASA Global Clime change Master Dictionary Science Keywords",
            "url": "https://gcmdservices.gsfc.nasa.gov/kms/concepts/concept_scheme/sciencekeywords/?format=xml"
        }
    ])
    # add thesaurus columns
    op.add_column('keywords', Column('thesaurus_id', Integer, ForeignKey('thesaurus.id')))

    # updat existing
    op.execute("UPDATE keywords SET thesaurus_id=1 WHERE id < 10000")


def downgrade():
    # drop the foreign key
    op.drop_column('keywords', 'thesaurus_id')
    # drop the thesaurus table
    op.drop_table('thesaurus')

    # add the alias and values fields
    op.add_column('nm_keywords_entries', Column('alias', String(1024)))
    op.add_column('nm_keywords_entries', Column('associated_value', String(1024)))
