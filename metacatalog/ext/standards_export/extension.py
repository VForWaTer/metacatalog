from __future__ import annotations
from typing import Union, overload
from typing_extensions import Literal
import os


from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree


from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.ext.standards_export.util import _parse_iso_information, _init_jinja, _validate_xml
from metacatalog import api
from metacatalog.models import Entry
from metacatalog.util.results import ImmutableResultSet


class StandardsExportExtension(MetacatalogExtensionInterface):
    r"""
    Extension to export Entries in standard format.
    Currently, ISO 19115 export is implemented.

    Adds the method standards_export to :class:`Entry <metacatalog.models.Entry>`
    which creates the metadata standard XML for the :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`
    of the Entry.
    The method standards_export is added to the API
    (metacatalog.api.catalog). This method can be
    used to export Entries / ImmutableResultSets
    in the database session and write the XML files
    to the folder location specified in ``path``.

    """
    @classmethod
    def init_extension(cls):
        # wrapper which calls StandardsExportExtension.standards_export
        def wrapper_entry(self: Entry, config_dict: dict, template_path: str) -> ET.ElementTree:  
            return StandardsExportExtension.standards_export(entry_or_resultset=self, config_dict=config_dict, template_path=template_path)
        
        # standards_export docstring and name for wrapper function
        wrapper_entry.__doc__ = StandardsExportExtension.standards_export.__doc__
        wrapper_entry.__name__ = StandardsExportExtension.standards_export.__name__
        
        # add wrapper to Entry model
        Entry.standards_export = wrapper_entry

        # add function create_iso19115 to api.catalog
        def wrapper_api(session: Session, config_dict: dict, path: str, if_exists: str = 'fail', verbose: bool = False):
            StandardsExportExtension.create_standard_metadata(session, config_dict, path, if_exists, verbose)

        wrapper_api.__doc__ = StandardsExportExtension.create_standard_metadata.__doc__
        wrapper_api.__name__ = StandardsExportExtension.create_standard_metadata.__name__

        # add wrapper to api.catalog
        api.catalog.create_standard_metadata = wrapper_api


    @classmethod
    def standards_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict: dict, template_path: str = './schemas/iso19115/iso19115-2.j2') -> ET.ElementTree:
        """
        Export a :class:`Entry <metacatalog.models.Entry>` or 
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>` to XML.
        The metadata standard is dependent on the jinja2 template passed to this function
        in template_path.
        Repeatable information input is always a list, as we can loop over the lists in the
        jinja2 template.
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
        template_path : str
            Full path (including the template name) to the jinja2 template for 
            metadata export.  
            Currently defaults to the ISO 19115 template.
        
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
        template = _init_jinja(template_path)

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

@overload 
def create_standard_metadata(path: str) -> None: ...
@overload
def create_standard_metadata(path: Literal[None]) -> ElementTree: ...
def create_standard_metadata(session: Session, id_or_uuid: Union[int, str], config_dict: dict, path: str = None, template_path: str = './schemas/iso19115/iso19115-2.j2') -> ElementTree | None:
        """
        This function can be imported from metacatalog.api.catalog

        Create standard metadata XML file for an entry, which is found 
        by its id or uuid. The metadata standard is determined by the
        jinja2 template passed in ``template_path``. Currently defaults
        to the ISO 19115 template.
        The XML file is saved to the folder given in ``path``. If ``path``
        does not end with '.xml', the name of the XML file is generated 
        with the uuid of the used ImmutableResultSet: 
        f"iso19115_{irs_uuid}.xml" 
        TODO: naming for other standards in the future.

        .. versionadded:: 0.7.8

        Parameters
        ----------
        session : sqlalchemy.Session
            SQLAlchemy session connected to the database.
        id_or_uuid : Union[int, str]
            id or uuid of the Entry to be exported.
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
        path : str
            Location where the ISO19115 XML file is saved to.
            If path ends with the name of the XML file (i.e. ends with '.xml'), the file is
            named as given.
            If path is a folder location, the name of the XML file is auto-generated with
            the uuid of the ImmutableResultSet of the entry: ``f"iso19115_{uuid}.xml".
            If no path is given, the class:`ElementTree <xml.etree.ElementTree.ElementTree>` object
            representing the XML ElementTree is returned.
        template_path : str
            Full path (including the template name) to the jinja2 template for 
            metadata export.  
            Currently defaults to the ISO 19115 template.

        Returns
        ----------
        xml : Union[ElementTree, None]
            If no path is given, the :class:`ElementTree <xml.etree.ElementTree.ElementTree>` object
            representing the XML ElementTree in Python is returned.
            If a path is given, None is returned.
        
        Notes
        ----------
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        a useful Metadata export.  

        """
        # find the entry by id
        if isinstance(id_or_uuid, int):
            entry = api.find_entry(session, id=id_or_uuid, return_iterator=True).first()
            # raise error if no entry was found
            if not entry:
                raise NoResultFound(f"No entry with id={id_or_uuid} was found.")

        # find the entry by uuid
        elif isinstance(id_or_uuid, str):
            entry = api.find_entry(session, uuid=id_or_uuid)
            # raise error if no entry was found
            if not entry:
                raise NoResultFound(f"No entry with uuid={id_or_uuid} was found.")

        # export entry to ISO 19115
        xml = entry.standards_export(config_dict=config_dict, template_path=template_path)

        if path:
            # get the uuid of the ImmutableResultSet that is written to ISO19115 XML (rs.group.uuid or rs.get('uuid'))
            irs_uuid = ImmutableResultSet(entry).uuid

            # use absolute path
            path = os.path.abspath(path)

            # if path does not end with .xml: auto-generate XML filename
            if not path.endswith('.xml'):
                path += f"/iso19115_{irs_uuid}.xml"

            # write XML file
            with open(path, 'wb') as f:
                xml.write(f, encoding='utf-8')
        else:
            return ET.ElementTree(ET.fromstring(xml))
        