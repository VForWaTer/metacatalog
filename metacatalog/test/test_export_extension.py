import pytest
import json

from metacatalog import api
from metacatalog.models import Entry
from ._util import connect


def find_composite_entry(session):
    """
    Find the Telegraph because it's a composite entry
    """
    entry = api.find_entry(session, title='Telegraph')[0]

    assert 'telegraph from 1855' in entry.abstract

    return entry


def export_to_json(entry: Entry):
    """
    Test the JSON export without data
    """
    json_str = entry.export(path=None, fmt='JSON', no_data=True)

    assert len(json_str) > 0

    # parse back
    d = json.loads(json_str)

    assert len(d['title']) == 3

    return True


@pytest.mark.depends(on=['groups'], name='export')
def test_export_extension():
    """
    Test export functionality by exporting and checking
    dtypes and file existance

    """
    session = connect(mode='session')

    # the first test returns the entry to be used for the others
    entry = find_composite_entry(session)

    # run tests
    assert export_to_json(entry)