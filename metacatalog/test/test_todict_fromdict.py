import pytest

from metacatalog import api
from metacatalog.models import Entry
from ._util import connect


def check_to_dict_persons(session):
    """
    Find an Entry with Persons with different roles.
    Check `Entry.to_dict()` output to behave as expected
    for different person roles.
    """
    # find entry
    entry = api.find_entry(session, title="Entry with some extra Person roles")[0]

    # Entry.to_dict()
    entry_dict = entry.to_dict()

    # check that the different roles are correctly displayed in Entry.to_dict()
    assert entry_dict['author']['last_name'] == 'Reeves'
    assert len(entry_dict['authors']) == 2
    assert len(entry_dict['pointOfContact']) == 2
    assert entry_dict['rightsHolder'][0]['last_name'] == 'Master'

    return True


def check_from_dict(session):
    """
    Check Entry.from_dict().
    This currently just checks that `Entry.from_dict(entry.to_dict())`
    raises a NotImplementedError, as 'id' is in the input dict. 
    """
    # find entry
    entry = api.find_entry(session, title="Entry with some extra Person roles")[0]

    # Entry.to_dict()
    entry_dict = entry.to_dict()

    # currently, if 'id' is in entry_dict, a NotImplementedError is raised
    with pytest.raises(NotImplementedError):
        # run Entry.from_dict()
        entry_fromdict = Entry.from_dict(session=session, data=entry_dict)

    return True


@pytest.mark.depends(on=['add_find'], name='dict_methods')
def test_fromdict_todict():
    """
    Currently tests Entry.to_dict() and Entry.from_dict()
    """
    # get a session
    session = connect(mode='session')

    # add an Entry with different Person roles associated to it
    entry = api.add_entry(session,
                          title="Entry with some extra Person roles",
                          author='Reeves',
                          location="SRID=4326;POINT (29 28)",
                          variable=1,
                          license='ODbL'
                          )

    # add Homer Simpson as coAuthor
    api.add_persons_to_entries(session, [entry], ['Simpson'], ['coAuthor'], [2])

    # Marie Curie and David Edward Hughes as pointOfContact
    api.add_persons_to_entries(session, [entry], ['Curie', 'Hughes'], ['pointOfContact', 'pointOfContact'], [3, 3])

    # Location Master as rightsHolder
    api.add_persons_to_entries(session, [entry], ['Master'], ['rightsHolder'], [4])

    # run single tests
    assert check_to_dict_persons(session)
    assert check_from_dict(session)
