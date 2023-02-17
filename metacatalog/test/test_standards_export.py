import xml.etree.ElementTree as ET

import pytest


from metacatalog import api, ext
from metacatalog.util.results import ImmutableResultSet
from ._util import connect


def check_entry_iso19115_export(session, config_dict, template_path):
    """
    Check if ``Entry.standards_export`` produces an ISO 19915 
    XML ElementTree object for all Entries in the test database.

    """
    for i, entry in enumerate(api.find_entry(session)):
        # get the ISO 19115 ElementTree of the Entry
        xml_etree = entry.standards_export(config_dict, template_path)

        assert isinstance(xml_etree, ET.ElementTree), f"[{i+1}] entry_id = {entry.id}: Entry did not return an ElementTree object."

    return True


def check_api_iso19115_export(session, config_dict, path):
    """
    Check if ``api.catalog.create_iso19115_xml`` creates an ISO
    19115 .xml file for all Entries in the test database.
    Also checks that the XML ElementTree object is returned if
    no path is given to ``create_iso19115_xml``.
    
    """    
    for i, entry in enumerate(api.find_entry(session)):
        # create xml
        api.catalog.create_iso19115_xml(session, id_or_uuid=entry.id, config_dict=config_dict, path=path)

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
        xml_etree = api.catalog.create_iso19115_xml(session, id_or_uuid=entry.uuid, config_dict=config_dict)

        assert isinstance(xml_etree, ET.ElementTree), f"[{i+1}] entry_id = {entry.id}: Entry did not return an ElementTree object."
    
    return True


@pytest.mark.depends(on=['add_find'], name='standards_export')
def test_standards_export(tmp_path):
    """
    Currently tests ISO 19115 export of Entry objects.

    """
    # get a session
    session = connect(mode='session')

    # activate standards_export extension, as long as it is not activated by default
    try:
        ext.extension('standards_export')
    except AttributeError:
        ext.activate_extension('standards_export', 'metacatalog.ext.standards_export', 'StandardsExportExtension')
        from metacatalog.ext.standards_export import StandardsExportExtension
        ext.extension('standards_export', StandardsExportExtension)

    # config_dict for V-For-WaTer as contact
    CONFIG_DICT = {
        "contact": {
            "organisationName": "Karlsruhe Institute of Technology (KIT) - Institute of Water and River Basin Management - Chair of Hydrology",
            "deliveryPoint": "Otto-Ammann-Platz 1, Building 10.81",
            "city": "Karlsruhe",
            "administrativeArea": "Baden-Wuerttemberg",
            "postalCode": "76131",
            "country": "Germany",
            "electronicMailAddress": ["alexander.dolich@kit.edu", "mirko.maelicke@kit.edu"],
            "linkage": "https://portal.vforwater.de/",
            "linkage_name": "V-FOR-WaTer",
            "linkage_description": "Virtual research environment for water and terrestrial environmental research"
        },
        "publisher": {
            "organisation_name": "KIT, V-For-WaTer online platform"
        }
    }

    # create temporary directory from pytest fixture tmp_path to store .xml files
    iso_xml_dir = tmp_path / 'iso_xml_dir'
    iso_xml_dir.mkdir()

    # run single tests
    assert check_entry_iso19115_export(session, config_dict=CONFIG_DICT, template_path='metacatalog/ext/standards_export/schemas/iso19115/iso19115-2.j2')
    assert check_api_iso19115_export(session, config_dict=CONFIG_DICT, path=iso_xml_dir)
