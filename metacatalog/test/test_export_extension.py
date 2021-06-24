import pytest
import json
import os
from random import choice
from string import ascii_letters
import shutil
import dicttoxml

from metacatalog import api
from metacatalog.models import Entry, EntryGroup
from ._util import connect


@pytest.fixture
def folder_location():
    # create a new folder
    fname = ''.join([choice(ascii_letters) for _ in range(10)])
    os.mkdir(f'./{fname}')

    # run tests
    yield f'./{fname}/'

    # remove the folder and content
    shutil.rmtree(f'./{fname}')


def find_composite_entry(session):
    """
    Find the Telegraph because it's a composite entry
    """
    entry = api.find_entry(session, title='Telegraph')[0]

    assert 'telegraph from 1855' in entry.abstract

    return entry

def find_composite_group(session):
    """
    Find the Telegraph and Microphone group.
    """
    group = api.find_group(session, title="Awesome inventions")[0]

    assert 'by David Edward Hughes' in group.description

    return group


def export_to_json(entry: Entry):
    """
    Test the JSON export without data
    """
    json_str = entry.export(path=None, fmt='JSON', no_data=True)

    assert len(json_str) > 0

    # parse back
    d = json.loads(json_str)

    # there is the partial entry now, which should be found
    assert len(d['title']) == 4

    return True


def export_to_json_file(entry: Entry, path: str):
    """
    Export to JSON file and verify the file exists
    """
    fpath = os.path.join(path, 'hughes.json')
    entry.export(path=fpath, fmt='JSON')

    assert os.path.exists(fpath)

    return True


def export_to_xml(group: EntryGroup):
    """
    Test the XML export without data
    """
    xml_str = group.export(path=None, fmt='fast_xml', no_data=True)

    assert len(xml_str) > 0

    assert '<title>' in xml_str

    return True


def export_to_xml_file(group: EntryGroup, path: str):
    """
    Export to XML file and verify it exists
    """
    fpath = os.path.join(path, 'inventions.xml')
    group.export(path=fpath, fmt='fast_xml')

    assert os.path.exists(fpath)

    return True


@pytest.mark.depends(on=['groups'], name='export')
def test_export_extension(folder_location):
    """
    Test export functionality by exporting and checking
    dtypes and file existance

    """
    session = connect(mode='session')

    # the first test returns the entry to be used for the others
    entry = find_composite_entry(session)

    # find the composite group
    group = find_composite_group(session)

    # run tests - Entry
    assert export_to_json(entry)
    assert export_to_json_file(entry, folder_location)

    # Composite
    assert export_to_xml(group)
    assert export_to_xml_file(group, folder_location)
