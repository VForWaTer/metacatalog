import pytest

from metacatalog import models
from metacatalog import api
from metacatalog.util.results import ImmutableResultSet
from ._util import connect


def add_person_and_entries(session):
    """
    Add a person using the model

    """
    hughes = models.Person(
        first_name='David Edward',
        last_name='Hughes',
        organisation_name='Royal Society',
    )
    e1 = models.Entry(
        title='Telegraph', abstract='Printing telegraph from 1855',
        location="SRID=4326;POINT (51.505946 -0.132951)", license_id=5,
        variable_id=1
    )
    e2 = models.Entry(
        title='Microphone', abstract='First microphone of 1878, dedicated to public domain.',
        location="SRID=4326;POINT (51.505946 -0.132951)", license_id=5,
        variable_id=1
    )
    
    e1.contributors.append(models.PersonAssociation(relationship_type_id=1, person=hughes, order=1))
    e2.contributors.append(models.PersonAssociation(relationship_type_id=1, person=hughes, order=1))

    session.add(hughes)
    session.add(e1)
    session.add(e2)
    session.commit()

    return e1.author.id == hughes.id and e2.author.id == hughes.id


def make_composite(session):
    e1 = session.query(models.Entry).filter(models.Entry.title=='Microphone').one()
    e2 = session.query(models.Entry).filter(models.Entry.title=='Telegraph').one()
    
    composite = e1.make_composite(
        title='Awesome inventions', 
        description='Invensions made by David Edward Hughes',
        others=[e2], commit=True
    )

    return len(composite.entries) == 2


def check_result_set(session):
    """
    Load only one of the entries and create a ImmutableResultSet
    """
    result = api.find_entry(session, title='Microphone', as_result=True)[0]

    assert isinstance(result, ImmutableResultSet)

    # there should be 3 uuids
    assert len(result.get('uuid')) == 3

    # but only one author
    assert isinstance(result.get('author'), dict)

    return True


def check_result_set_from_group(session):
    """
    Create a ImmutableResultSet from a group
    """
    result = api.find_group(session, title='Awesome inventions', as_result=True)[0]

    # assert like above
    assert len(result.get('uuid')) == 3

    return True


@pytest.mark.depends(on=['db_init'], name='groups')
def test_groups_and_projects():
    """
    """
    session = connect(mode='session')

    # run tests
    assert add_person_and_entries(session)
    assert make_composite(session)
    assert check_result_set(session)
    assert check_result_set_from_group(session)

