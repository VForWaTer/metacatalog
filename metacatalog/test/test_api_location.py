import pytest

from ._util import connect, read_to_df
from metacatalog import api

PERSONS = """first_name,last_name,organisation_name,affiliation
Location,Master,"Insitute for Geoinformatics","Department for Location"
"""
ENTRIES = """title,author,x,y,variable,abstract,license
"Van Gogh",Master,4.880941145937998,52.35853204312844,1,"Van Gogh Museum",5
"Stedelijk",Master,4.8796320311376205,52.358157919933284,1,"Stedelijk Museum",5
"Rijksmuseum",Master,4.885228409057987,52.36000114096789,1,"Rijksmuseum",5
"Anne Frank",Master,4.884012153648794,52.37553731587671,1,'Anne Frank Huis",5
"""


def add_data(session):
    p_df = read_to_df(PERSONS)
    persons = [api.add_person(session, **p) for p in p_df.to_dict(orient='records')]

    e_df = read_to_df(ENTRIES)
    e_df['location'] = [(t[0], t[1]) for t in e_df[['x', 'y']].values]
    e_df = e_df.where(e_df.notnull(), None)
    e_df.drop(['x', 'y'], axis=1, inplace=True)

    entries = [api.add_entry(session, **e) for e in e_df.to_dict(orient='records')]

    return True


def get_entry_neighbors(session):
    # get the Van Gogh Museum
    gogh = api.find_entry(session, title="Van Gogh", return_iterator=True).one()

    # find only one - Stedelijk
    neighs = gogh.neighbors(distance=200)

    assert len(neighs) == 1
    assert neighs[0].title == 'Stedelijk'

    # find Rijksmuseum as well
    more = gogh.neighbors(distance=550)
    assert len(more) == 2

    return True


@pytest.mark.depends(on=['db_init'], name='api_location')
def test_api_location():
    """
    Adds a few dummy records and finds them by location.
    """
    # get a session
    session = connect(mode='session')
    
    # run the tests
    assert add_data(session)
    assert get_entry_neighbors(session)
