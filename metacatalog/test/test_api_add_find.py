import pytest
from uuid import uuid4

from sqlalchemy.orm.exc import NoResultFound

from metacatalog import api, models
from ._util import connect, PATH, read_to_df

PERSONS = """first_name,last_name,organisation_name,affiliation,attribution
Keanu,Reeves,"Institute for martial arts","Institute for martial arts - department of slow motion",
Marie,Curie,"Institute of awesome scientists","Insitute of awesome scientists - department for physics","Curie, Marie, Awesome."
Homer,Simpson,"University of Non-existent people",,
"""

ENTRIES = """title,author,x,y,variable,abstract,license,external_id,uuid
"Dummy 1",Reeves,13,44.5,5,"Lorem ipsum ..",5,abc,4722782d-6bcb-463f-a1eb-6cebe490e9c4
"Dummy 2",Curie,12,41.8,6,"Another dummy entry about abosulte nothing",4,foobar,
"Dummy 3",Reeves,10.5,44.5,6,"Another dummy entry by Keanu reeves",4,foobar2,
"""

def add_person(session):
    """
    Add few persons to db
    """
    df = read_to_df(PERSONS)
    person_dict_list = df.to_dict(orient='records')
    
    # add 
    persons = [api.add_person(session, **p) for p in person_dict_list]

    # assert
    df.set_index('first_name', inplace=True)
    for person in persons:
        assert person.last_name == df.loc[person.first_name].last_name
    
    assert persons[1].attribution == "Curie, Marie, Awesome."
    
    return True


def add_entries(session):
    """
    Add few the entries
    """
    df = read_to_df(ENTRIES)
    df['location'] = [(t[0], t[1],) for t in df[['x', 'y']].values]
    # get rid of the uuids
    df = df.where(df.notnull(), None)
    df.drop(['x', 'y'], axis=1, inplace=True)
    entries_dict_list = df.to_dict(orient='records')

    # add
    entries = [api.add_entry(session, **e) for e in entries_dict_list]

    # assert
    assert entries[0].contributors[0].person.first_name == 'Keanu'
    assert entries[1].abstract == df.loc[1].abstract
    assert entries[2].external_id == 'foobar2'

    return True


def add_details(session):
    """
    Add some random detail
    """
    # find the first entry
    e1 = api.find_entry(session, title="Dummy 1")[0]
    e2 = api.find_entry(session, title="Dummy 2")[0]

    # check compability of < v0.1.8 api
    api.add_details_to_entries(session, [e1], **{'foo': 'bar 1'})
    api.add_details_to_entries(session, [e2], **{'foo': 'bar 2'})
    api.add_details_to_entries(session, [e1, e2], **{'banana': 'both love it'})
    
    # check the new possibilites:
    api.add_details_to_entries(session, [e1], 
        details=[dict(key='baz', value=42, description='Baz is the best kw')])

    d = e1.details_dict(full=True)
    assert d['baz']['description']=='Baz is the best kw'
    
    return True


def associate_persons(session):
    """
    Set the others as associated persons
    """
    e1 = api.find_entry(session, title='Dummy 1')[0]
    e2 = api.find_entry(session, external_id='foobar')[0]

    assert e1.id != e2.id

    # make Homer a coauthor to both
    api.add_persons_to_entries(session, [e1, e2], ['Simpson'], 13, 2)

    # and Keanu also contributed to e2 as a contributor
    api.add_persons_to_entries(session, [e2], ['Reeves'], 15, 3)

    return True


def check_related_information(session):
    """
    Run some assert statements on related information
    
    """
    e1 = api.find_entry(session, title="Dummy 1")[0]
    e2 = api.find_entry(session, title="Dummy 2")[0]

    # do some tests
    assert len(e1.contributors) == 2
    assert len(e2.contributors) == 3

    # details 
    e1_details = e1.details_dict(full=True)
    assert 'banana' in e1_details.keys()
    assert e1_details['foo']['value'] == 'bar 1'

    return True


def check_find_with_wildcard(session):
    """
    Check the wildcard pattern added in version 0.1.8
    """
    # match all titles
    entries = api.find_entry(session, title='Dummy *')
    assert len(entries) == 3

    # match only the two abstracts
    entries = api.find_entry(session, abstract='%entry%')
    assert len(entries) == 2
    assert entries[0].author.last_name == 'Curie'

    entries = api.find_entry(session, abstract='!*entry*')
    assert not any([e.title in ('Dummy 2', 'Dummy 3') for e in entries])
    assert any([e.author.first_name == 'Keanu' for e in entries])

    return True


def check_has_uuid(session):
    """
    load UUID of all Entries and check that 
    they are all different
    """
    entries = api.find_entry(session)
    uuids = [e.uuid for e in entries]

    assert len(uuids) > 0
    assert len(uuids) == len(set(uuids))

    return True


def add_project_group(session):
    """
    Group two entries into a group
    """
    # get the entries
    e1 = api.find_entry(session, title="Dummy 1")[0]
    e2 = api.find_entry(session, title="Dummy 2")[0]

    # get the project type
    group_type = api.find_group_type(session, name='Composite')[0]

    # create a EntryGroup
    # TODO this needs an api endpoint. until then the models are used
    project = models.EntryGroup(title='Dummies', type=group_type, description='empty')
    project.entries.append(e1)
    project.entries.append(e2)
    session.add(project)
    session.commit()

    return True


def check_project_group(session):
    """
    get the group. Check UUID exists and two entries are connected
    """
    dummies = api.find_group(session, title="Dumm*")[0]

    assert len(dummies.uuid) == 36
    assert len(dummies.entries) == 2

    return True


def check_composite_raises_error(session):
    with pytest.raises(TypeError) as excinfo:
        dummies = api.find_group(session, title="Dumm%")[0]
        api.find_entry(session, project=dummies)
    
    return "has to be of type 'Project'" in str(excinfo.value)

def find_by_project(session):
    dummies = api.find_group(session, title="Dumm%")[0]
    # create a project
    project = api.add_project(session, 
        entry_ids=[e.id for e in dummies.entries],
        title='Awesome Project', description='Nice project for testing purposes'
    )

    for pars in [dict(project=project), dict(project=project.id), dict(project='Awesome%')]:
        entries = api.find_entry(session, **pars)
        assert len(entries) == 2
        assert set([e.title for e in entries]) == set(['Dummy 1', 'Dummy 2'])
    
    return True


def check_get_by_uuid(session):
    """
    Check if the keyword of UUID 5f2ec7b9-3e8c-4d12-bba6-0f84c08729e0
    can be found by UUID and is the correct one.

    """
    kw_uuid = '5f2ec7b9-3e8c-4d12-bba6-0f84c08729e0'
    e_uuid = '4722782d-6bcb-463f-a1eb-6cebe490e9c4'

    keyword = api.get_uuid(session, uuid=kw_uuid)
    entry = api.get_uuid(session, uuid=e_uuid)

    # correct keyword found
    assert keyword.id == 5890
    assert keyword.value == 'EXTINCTION COEFFICIENTS'

    # correct entry found
    assert entry.abstract == "Lorem ipsum .."

    # a newly created UUID should not be found
    with pytest.raises(NoResultFound):
        api.get_uuid(session, uuid=str(uuid4()))

    return True


def find_by_author(session):
    entries = api.find_entry(session, author='Reev*')

    assert len(entries) == 2
    assert set([e.title for e in entries]) == set(['Dummy 1', 'Dummy 3'])
    return True


@pytest.mark.depends(on=['db_init'], name='add_find')
def test_add_and_find():
    """
    A simple workflow of 3 persons who contributed to two entries.
    The content of some related content is tested randomly
    """
    # get a session
    session = connect(mode='session')

    # run single tests
    assert add_person(session)
    assert add_entries(session)
    assert add_details(session)
    assert associate_persons(session)
    assert check_related_information(session)
    assert check_find_with_wildcard(session)
    assert check_has_uuid(session)
    assert add_project_group(session)
    assert check_composite_raises_error(session)
    assert check_project_group(session)
    assert find_by_project(session)
    assert check_get_by_uuid(session)
    assert find_by_author(session)
