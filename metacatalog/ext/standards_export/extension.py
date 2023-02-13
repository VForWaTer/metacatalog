from typing import Union
import os


from tqdm import tqdm
from sqlalchemy.orm import Session
import xml.etree.ElementTree as ET


from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.ext.standards_export.util import _parse_iso_information, _init_iso19115_jinja, _validate_xml
from metacatalog import api, cmd
from metacatalog.models import Entry
from metacatalog.util.results import ImmutableResultSet


class StandardsExportExtension(MetacatalogExtensionInterface):
    r"""
    Extension to export Entries in standard format.
    Currently, ISO 19115 export is implemented.

    Adds the method export_iso19115 to :class:`Entry <metacatalog.models.Entry>`
    which creates the ISO 19115 XML for the :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`
    of the Entry.
    The method create_iso19115 is added to the API
    (metacatalog.api.catalog). This method can be
    used to export all Entries / ImmutableResultSets
    in the database session and write the XML files
    to the folder location specified in ``path``.

    """
    @classmethod
    def init_extension(cls):
        # wrapper which calls StandardsExportExtension.iso19115_export
        def wrapper_entry(self: Entry, config_dict: dict):  
            return StandardsExportExtension.iso19115_export(entry_or_resultset=self, config_dict=config_dict)
        
        # iso19115_export docstring and name for wrapper function
        wrapper_entry.__doc__ = StandardsExportExtension.iso19115_export.__doc__
        wrapper_entry.__name__ = StandardsExportExtension.iso19115_export.__name__
        
        # add wrapper to Entry model
        Entry.export_iso19115 = wrapper_entry


    @classmethod
    def iso19115_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict: dict):
        """
        Export a :class:`Entry <metacatalog.models.Entry>` or 
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>` to XML in 
        ISO 19115 standard.
        Repeatable information input is always a list, as we can loop over the lists in the
        jinja ISO 19115 template.
        Always returns an :class:`ElementTree <xml.etree.ElementTree.ElementTree>` object.

        This function is added as a method to :class:`Entry <metacatalog.models.Entry>`
        when the extension is activated. 

        .. versionadded:: 0.7.7

        Parameters
        ----------
        entry_or_resultset : Union[Entry, ImmutableResultSet]
            The entry instance to be exported
        config_dict : dict
            Configuration dictionary, containing information about the data provider.
            Mandatory (nested) keys and type of values:

            .. code-block:: Python

            dict(
                contact: dict(
                    organisationName: str = '',
                    deliveryPoint: str = '',
                    city: str = '',
                    administrativeArea: str = '',
                    postalCode: str = '',
                    country: str = '',
                    electronicMailAddress: list(str) = ['', ''],
                    linkage: str = '',
                    linkage_name: str = '',
                    linkage_description: str = ''
                ),
                publisher: dict(
                    organisation_name: str = ''
                )
            )
        
        Returns
        ----------
        xml : xml.etree.ElementTree.ElementTree
            The :class:`ElementTree <xml.etree.ElementTree.ElementTree>` object
            representing the XML ElementTree in Python.

        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        a useful Metadata export.

        """
        # get necessary input parameters from ImmutableResultSet for ISO export
        iso_input = _parse_iso_information(entry_or_resultset)

        # get initialized jinja template
        template = _init_iso19115_jinja()

        # render template with entry_dict
        xml = template.render(**iso_input, **config_dict)

        # check whether xml is well-formed
        assert _validate_xml(xml)

        # register namespaces for ElementTree representation of XML
        ET.register_namespace('gmi', 'http://www.isotc211.org/2005/gmi')
        ET.register_namespace('gco', 'http://www.isotc211.org/2005/gco')
        ET.register_namespace('gmd', 'http://www.isotc211.org/2005/gmd')
        ET.register_namespace('gml', 'http://www.opengis.net/gml/3.2')
        ET.register_namespace('xlink', 'http://www.w3.org/1999/xlink')

        # convert to ElementTree and return
        return ET.ElementTree(ET.fromstring(xml))
