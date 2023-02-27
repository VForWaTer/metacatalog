import subprocess
import os

import pytest
import xml.etree.ElementTree as ET


from metacatalog import api, ext
from metacatalog.util.results import ImmutableResultSet
from ._util import connect


def check_entry_iso19115_export(session, template_path):
    """
    Check if ``Entry.standards_export`` produces an ISO 19915 
    XML ElementTree object for all Entries in the test database.

    """
    for i, entry in enumerate(api.find_entry(session)):
        # get the ISO 19115 ElementTree of the Entry
        xml_etree = entry.standards_export(template_path=template_path)

        assert isinstance(xml_etree, ET.ElementTree), f"[{i+1}] entry_id = {entry.id}: Entry did not return an ElementTree object."

    return True


def check_api_iso19115_export(session, path):
    """
    Check if ``api.catalog.create_iso19115_xml`` creates an ISO
    19115 .xml file for all Entries in the test database.
    Also checks that the XML ElementTree object is returned if
    no path is given to ``create_iso19115_xml``.
    
    """    
    for i, entry in enumerate(api.find_entry(session)):
        # create xml
        api.catalog.create_iso19115_xml(session, id_or_uuid=entry.id, path=path)

        # get ImmutableResultSet uuid to open file
        irs_uuid = ImmutableResultSet(entry).uuid

        # .xml filenames are generated from ImmutableResultSet.uuid
        with open(f"{path}/iso19115_{irs_uuid}.xml") as f:
            xml_str = f.read()
            
            # check expected start of the xml string
            assert xml_str.startswith("<gmi:MI_Metadata"), f"[{i+1}] entry_id = {entry.id}: XML file does not start with '<gmi:MI_Metadata'\nCheck file {path}/iso19115_{irs_uuid}.xml"

            # check if ImmurableResultSet.uuid is in xml_str
            assert irs_uuid in xml_str, f"[{i+1}] entry_id = {entry.id}: uuid is not contained in XML."

        # additionally check that create_iso19115 returns an ElementTree object if no path is given
        xml_etree = api.catalog.create_iso19115_xml(session, id_or_uuid=entry.uuid)

        assert isinstance(xml_etree, ET.ElementTree), f"[{i+1}] entry_id = {entry.id}: Entry did not return an ElementTree object."
    
    return True


def check_cli_iso19115_export(session, dburi, path):
    """
    Check if an individual entry is correctly exported by the 
    CLI and check if the flag ``--all`` leads to the export
    of all Entries / ImmuableResultSets in the test database.
    
    """
    # find test entry with title 3-dimensional windspeed data
    entry = api.find_entry(session, title='3-dimensional windspeed data')[0]

    # use entry id in CLI command
    cmd = ['python', '-m', 'metacatalog', 'standards-export', str(entry.id), '--format', 'iso19115', '--path', path, '--connection', dburi]
    
    # run command
    subprocess.run(cmd)

    # there must be exactly one file in the cli xml test directory
    assert len(os.listdir(path)) == 1

    # get xml filename
    xml_file = os.listdir(path)[0]

    # read xml file
    with open(f"{path}/{xml_file}") as f:
        xml_str = f.read()
            
        # check if the expected title is in xml string
        assert '3-dimensional windspeed data' in xml_str

    # test --all flag, which exports all entries (ImmutableResultSets) in the test database
    path_all = f"{path}/export_all"
    os.mkdir(path_all)

    cmd = ['python', '-m', 'metacatalog', 'standards-export', '--format', 'iso19115', '--all', '--path', path_all, '--connection', dburi]

    # run command
    subprocess.run(cmd)

    # time of development: 15 xml files expected for 15 ImmutableResultSets in the test database
    assert len(os.listdir(path_all)) == len(api.find_entry(session, as_result=True))

    return True


@pytest.mark.depends(on=['add_find'], name='standards_export')
def test_standards_export(tmp_path):
    """
    Currently tests ISO 19115 export of Entry objects.

    """
    # get a session
    session = connect(mode='session')

    # session string to test CLI
    dburi = connect(mode='string')

    # activate standards_export extension, as long as it is not activated by default
    try:
        ext.extension('standards_export')
    except AttributeError:
        ext.activate_extension('standards_export', 'metacatalog.ext.standards_export', 'StandardsExportExtension')
        from metacatalog.ext.standards_export import StandardsExportExtension
        ext.extension('standards_export', StandardsExportExtension)

    # create temporary directory from pytest fixture tmp_path to store .xml files
    iso_xml_dir_api = tmp_path / 'iso_xml_dir_api'
    iso_xml_dir_api.mkdir()

    iso_xml_dir_cli = tmp_path / 'iso_xml_dir_cli'
    iso_xml_dir_cli.mkdir()

    # run single tests
    assert check_entry_iso19115_export(session, template_path='metacatalog/ext/standards_export/schemas/iso19115/iso19115-2.j2')
    assert check_api_iso19115_export(session, path=iso_xml_dir_api)
    assert check_cli_iso19115_export(session, dburi, path=iso_xml_dir_cli)
