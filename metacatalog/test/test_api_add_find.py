import pytest



from metacatalog import api
from ._util import connect, PATH, read_to_df

PERSONS = """first_name,last_name,affiliation
Keanu,Reeves,"Institute for matrial arts"
Marie,Curie,"Institute of awesome scientists"
Homer,Simpson,"University of Non-existent people"
"""

ENTRIES = """title,author,x,y,variable,abstract,license,external_id
"Dummy 1",Reeves,13,44.5,5,"Lorem ipsum ..",2,abc
"Dummy 2",Curie,12,41.8,6,"Another dummy entry about abosulte northing",1,foobar
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
    
    return True


def add_entries(session):
    """
    Add few the entries
    """
    df = read_to_df(ENTRIES)
    df['location'] = [(t[0], t[1],) for t in df[['x', 'y']].values]
    df.drop(['x', 'y'], axis=1, inplace=True)
    entries_dict_list = df.to_dict(orient='records')

    # add
    entries = [api.add_entry(session, **e) for e in entries_dict_list]

    # assert
    assert entries[0].contributors[0].person.first_name == 'Keanu'
    assert entries[1].abstract == df.loc[1].abstract

    return True


def add_details(session):
    """
    Add some random detail
    """
    # find the first entry
    e1 = api.find_entry(session, title="Dummy 1")[0]
    e2 = api.find_entry(session, title="Dummy 2")[0]

    api.add_details_to_entries(session, [e1], **{'foo': 'bar 1'})
    api.add_details_to_entries(session, [e2], **{'foo': 'bar 2'})
    api.add_details_to_entries(session, [e1, e2], **{'banana': 'both love it'})

    return True


def associate_persons(session):
    """
    Set the others as associated persons
    """
    e1 = api.find_entry(session, id=1)[0]
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


@pytest.mark.depends(on=['db_init'])
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
