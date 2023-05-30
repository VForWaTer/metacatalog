import subprocess
import os

import pytest
import xml.etree.ElementTree as ET


from metacatalog import api, ext
from metacatalog.ext.standards_export.util import _get_title
from metacatalog.util.results import ImmutableResultSet
from ._util import connect


def check_entry_standards_export(session, template_path):
    """
    Check if ``Entry.standards_export`` produces an XML ElementTree 
    object according to the ``template_path`` for all Entries in 
    the test database.

    """
    for i, entry in enumerate(api.find_entry(session)):
        # get the xml ElementTree of the Entry
        xml_etree = entry.standards_export(template_path=template_path)

        assert isinstance(xml_etree, ET.ElementTree), f"[{i+1}] entry_id = {entry.id}: Entry did not return an ElementTree object."

    return True


def check_api_standards_export(session, path, template_path):
    """
    Check if ``api.catalog.create_iso19115_xml`` creates .xml file 
    according to the ``template_path`` for all Entries in the test 
    database.
    Also checks that the XML ElementTree object is returned if
    no path is given to ``api.catalog.create_standards_xml``.
    
    """    
    for i, entry in enumerate(api.find_entry(session)):
        # create xml
        api.catalog.create_standards_xml(session, id_or_uuid=entry.id, path=path, template_path=template_path)

        # get ImmutableResultSet uuid to open file
        rs = ImmutableResultSet(entry)
        irs_uuid = rs.uuid

        # .xml filenames are generated from ImmutableResultSet.uuid
        if 'iso19115' in template_path:
            with open(f"{path}/iso19115_{irs_uuid}.xml") as f:
                xml_str = f.read()

        elif 'datacite' in template_path:
            with open(f"{path}/datacite_{irs_uuid}.xml") as f:
                xml_str = f.read()
                
        # check if ImmurableResultSet title is in xml_str
        assert _get_title(rs) in xml_str, f"[{i+1}] entry_id = {entry.id}: title is not contained in XML."

        # additionally check that create_iso19115 returns an ElementTree object if no path is given
        xml_etree = api.catalog.create_standards_xml(session, id_or_uuid=entry.uuid, template_path=template_path)

        assert isinstance(xml_etree, ET.ElementTree), f"[{i+1}] entry_id = {entry.id}: Entry did not return an ElementTree object."
    
    return True


def check_api_strict_mode(session, path, template_path):
    """
    Check if the correct exception is raised for the different 
    metadata standards if using strict mode.  
    When strict mode is properly implemented with the validation
    of XML contents, this test needs to be adapted.

    """
    # ISO 19115 should raise a NotImplementedError (v0.8.2)
    if 'iso19115' in template_path:
        with pytest.raises(NotImplementedError, match="You want to use strict mode for the generation of ISO 19115 metadata, the generated XML structure is well-formed but its content currently cannot be validated."):
            # attempt to create xml
            api.catalog.create_standards_xml(session, id_or_uuid=10, path=path, template_path=template_path, strict=True)
    # DataCite should raise a ValueError (v0.8.2)
    elif 'datacite' in template_path:
        with pytest.raises(ValueError, match="You want to use strict mode for the generation of DataCite metadata, as metacatalog currently does not provide DOIs, the content of the generated XML file is not valid, as the DOI field is empty. Set strict=False to generate the XML nevertheless."):
            # attempt to create xml
            api.catalog.create_standards_xml(session, id_or_uuid=10, path=path, template_path=template_path, strict=True)

    return True

def check_cli_standards_export(session, dburi, format, path):
    """
    Check if an individual entry is correctly exported by the 
    CLI and check if the flag ``--all`` leads to the export
    of all Entries / ImmuableResultSets in the test database.
    
    """
    # find test entry with title 3-dimensional windspeed data
    entry = api.find_entry(session, title='3-dimensional windspeed data')[0]
    
    # create dictionary to store .xml file of single entry export
    path_one = f"{path}/export_one_{format}"

    os.mkdir(path_one)

    # use entry id in CLI command
    cmd = ['python', '-m', 'metacatalog', 'standards-export', str(entry.id), '--format', format, '--path', path_one, '--connection', dburi]

    # run command
    subprocess.run(cmd)

    # there must be exactly one file in the cli xml test directory
    assert len(os.listdir(path_one)) == 1

    # get xml filename
    xml_file = os.listdir(path_one)[0]

    # read xml file
    with open(f"{path_one}/{xml_file}") as f:
        xml_str = f.read()
            
        # check if the expected title is in xml string
        assert '3-dimensional windspeed data' in xml_str

    # test --all flag, which exports all entries (ImmutableResultSets) in the test database
    path_all = f"{path}/export_all_{format}"

    os.mkdir(path_all)

    cmd = ['python', '-m', 'metacatalog', 'standards-export', '--format', format, '--all', '--path', path_all, '--connection', dburi]

    # run command
    subprocess.run(cmd)

    # time of development: 15 xml files expected for 15 ImmutableResultSets in the test database
    assert len(os.listdir(path_all)) == len(api.find_entry(session, as_result=True))

    return True


@pytest.mark.depends(on=['add_find'], name='standards_export')
def test_standards_export(tmp_path):
    """
    Tests metadata export of Entry objects in ISO 19115
    and DataCite format.

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
    export_xml_dir_api = tmp_path / 'iso_xml_dir_api'
    export_xml_dir_api.mkdir()

    iso_xml_dir_cli = tmp_path / 'iso_xml_dir_cli'
    iso_xml_dir_cli.mkdir()

    # run single tests
    assert check_entry_standards_export(session, template_path='metacatalog/ext/standards_export/schemas/iso19115/iso19115-2.j2')
    assert check_entry_standards_export(session, template_path='metacatalog/ext/standards_export/schemas/datacite/datacite.j2')
    assert check_api_standards_export(session, path=export_xml_dir_api, template_path='metacatalog/ext/standards_export/schemas/iso19115/iso19115-2.j2')
    assert check_api_standards_export(session, path=export_xml_dir_api, template_path='metacatalog/ext/standards_export/schemas/datacite/datacite.j2')
    assert check_api_strict_mode(session, path=export_xml_dir_api, template_path='metacatalog/ext/standards_export/schemas/iso19115/iso19115-2.j2')
    assert check_api_strict_mode(session, path=export_xml_dir_api, template_path='metacatalog/ext/standards_export/schemas/datacite/datacite.j2')
    assert check_cli_standards_export(session, dburi, format='iso19115', path=iso_xml_dir_cli)
    assert check_cli_standards_export(session, dburi, format='datacite', path=iso_xml_dir_cli)
