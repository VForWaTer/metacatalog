import xml.etree.ElementTree as ET

import pytest


from metacatalog import api, ext
from ._util import connect


def check_entry_iso19115_export(session, config_dict, template_path):
    """
    Check if ``Entry.standards_export`` produces a ISO 19915 
    XML ElementTree object for all Entries in the test database.

    """
    for entry in api.find_entry(session):
        xml = entry.standards_export(config_dict, template_path)

        assert isinstance(xml, ET.ElementTree)

    return True


@pytest.mark.depends(on=['add_find'], name='standards_export')
def test_standards_export():
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

    # run single tests
    assert check_entry_iso19115_export(session, config_dict=CONFIG_DICT, template_path='metacatalog/ext/standards_export/schemas/iso19115/iso19115-2.j2')
