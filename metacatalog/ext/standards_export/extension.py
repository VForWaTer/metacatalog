from __future__ import annotations
from typing import Union, overload
from typing_extensions import Literal
import os
import json


from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
import xml.etree.ElementTree as ET
from tqdm import tqdm


from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.ext.standards_export.util import _parse_iso_information, _init_jinja, _validate_xml
from metacatalog import api
from metacatalog.models import Entry
from metacatalog.util.results import ImmutableResultSet
from metacatalog.cmd._util import connect, cprint


# default dictionary with the expected structure and keys and dummy values
DEFAULT_CONTACT = dict(
    contact = dict(
        organisationName = 'METACATALOG',
        deliveryPoint = 'YOUR ADRESS',
        city = 'YOUR CITY',
        administrativeArea = 'YOUR STATE',
        postalCode = '12345',
        country = 'YOUR COUNTRY',
        electronicMailAddress = ['JANE.DOE@EMAIL.COM', 'JOHN.DOE@EMAIL.COM'],
        linkage = 'HTTPS://YOUR.URL.COM/',
        linkage_name = 'YOUR WEBSITE NAME',
        linkage_description = 'YOUR WEBSITE DESCRIPTION'
        ),
    publisher = dict(
        organisation_name = 'METACATALOG'
        ))


# default template_path to the iso19115 jinja template, can be overwritten in functions with parameter template_path
TEMPLATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'schemas', 'iso19115', 'iso19115-2.j2'))


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
        def wrapper_entry(self: Entry, config_dict: dict = {}, template_path: str = TEMPLATE_PATH) -> ET.ElementTree:  
            return StandardsExportExtension.standards_export(entry_or_resultset=self, config_dict=config_dict, template_path=template_path)
        
        # standards_export docstring and name for wrapper function
        wrapper_entry.__doc__ = StandardsExportExtension.standards_export.__doc__
        wrapper_entry.__name__ = StandardsExportExtension.standards_export.__name__
        
        # add wrapper to Entry model
        Entry.standards_export = wrapper_entry

        # add function create_iso19115 to api.catalog
        def wrapper_api(session: Session, id_or_uuid: Union[int, str], config_dict: dict = {}, path: str = None, template_path: str = TEMPLATE_PATH):
            return StandardsExportExtension.create_iso19115_xml(session, id_or_uuid, config_dict, path, template_path)

        wrapper_api.__doc__ = StandardsExportExtension.create_iso19115_xml.__doc__
        wrapper_api.__name__ = StandardsExportExtension.create_iso19115_xml.__name__

        # add wrapper to api.catalog
        api.catalog.create_iso19115_xml = wrapper_api


    @classmethod
    def init_cli(cls, subparsers, defaults):
        myparser = subparsers.add_parser('standards-export', parents=[defaults], help="Export metadata in standard format as .xml files.")
        myparser.add_argument('entries', nargs='*', help='ID(s) or UUID(s) of Entries to export.')
        myparser.add_argument('--format', choices=['iso19115'], type=str, nargs='?', const='iso19115', default='iso19115', help="Metadata standard format.")
        myparser.add_argument('--path', type=str, help="Directory to save XML file(s) to, `if not specified, the current folder is used.")
        myparser.add_argument('--all', action='store_true', help="Export all entries in the session to ISO 19115, cannot be used together with --id or --uuid.")
        myparser.set_defaults(func=StandardsExportExtension.cli_create_standards_xml)


    @classmethod
    def standards_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict: dict = {}, template_path: str = TEMPLATE_PATH) -> ET.ElementTree:
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
            The following keys and their values are expected when rendering the 
            jinja template:

            .. code-block:: Python

            dict(
                contact = dict(
                    organisationName = '',
                    deliveryPoint = '',
                    city = '',
                    administrativeArea = '',
                    postalCode = '',
                    country = '',
                    electronicMailAddress = ['', ''],
                    linkage = '',
                    linkage_name = '',
                    linkage_description = ''
                ),
                publisher = dict(
                    organisation_name = ''
                ))
            
            It is also possible to create a .json file ``iso19115_contact.json`` containing the
            contact information and add the path to this file to the metacatalog CONFIGFILE under 
            the top level key ``extra``:

            .. code-block:: json

            "extra":{
    	        "iso19115_contact": "/path/to/iso19115_contact.json"
                }

            This is the only way to add the contact information if you use the metacatalog CLI
            for the export of metadata standards.
        template_path : str
            Full path (including the template name) to the jinja2 template for 
            metadata export.  
            Currently defaults to the ISO 19115 template.
        
        Returns
        ----------
        xml_etree : xml.etree.ElementTree.ElementTree
            The :class:`ElementTree <xml.etree.ElementTree.ElementTree>` object
            representing the XML ElementTree in Python.

        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        a useful Metadata export.

        """
        from metacatalog import CONFIGFILE

        # use dummy values for contact as default
        contact_config = DEFAULT_CONTACT.copy()

        # get contact config from metacatalog CONFIGFILE if specified
        with open(CONFIGFILE, 'r') as f:
            config = json.load(f)

            # get base_config path from CONFIGFILE: path to user generated .json with contact info
            base_config_path = config.get('extra', {}).get('iso19115_contact', '')

            if base_config_path:
                with open(base_config_path, 'r') as f:
                    base_config = json.load(f)
            else:
                base_config = {}

        # update default config with contact info from CONFIGFILE
        contact_config.update(base_config)

        # update config with config passed to this function in config_dict
        contact_config.update(config_dict)

        # get necessary input parameters from ImmutableResultSet for ISO export
        iso_input = _parse_iso_information(entry_or_resultset)

        # get initialized jinja template
        template = _init_jinja(template_path)

        # render template with entry_dict
        xml_str = template.render(**iso_input, **contact_config)

        # check whether xml is well-formed
        assert _validate_xml(xml_str)

        # register namespaces for ElementTree representation of XML
        ET.register_namespace('gmi', 'http://www.isotc211.org/2005/gmi')
        ET.register_namespace('gco', 'http://www.isotc211.org/2005/gco')
        ET.register_namespace('gmd', 'http://www.isotc211.org/2005/gmd')
        ET.register_namespace('gml', 'http://www.opengis.net/gml/3.2')
        ET.register_namespace('xlink', 'http://www.w3.org/1999/xlink')

        # convert to ElementTree and return
        return ET.ElementTree(ET.fromstring(xml_str))

    @overload
    def create_iso19115_xml(path: str) -> None: ...
    @overload
    def create_iso19115_xml(path: Literal[None]) -> ET.ElementTree: ...
    def create_iso19115_xml(session: Session, id_or_uuid: Union[int, str], config_dict: dict = {}, path: str = None, template_path: str = TEMPLATE_PATH) -> ET.ElementTree | None:
        """
        This function can be imported from metacatalog.api.catalog

        Create ISO 19115 standard metadata XML file for an entry, which is 
        found by its id or uuid.
        The XML file is saved to the folder given in ``path``. If ``path``
        does not end with '.xml', the name of the XML file is generated 
        with the uuid of the used ImmutableResultSet: 
        f"iso19115_{irs_uuid}.xml".
        If no ``path`` is given, the ``ElementTree`` XML representation
        is returned instead. 

        .. versionadded:: 0.8.1

        Parameters
        ----------
        session : sqlalchemy.Session
            SQLAlchemy session connected to the database.
        id_or_uuid : Union[int, str]
            id or uuid of the Entry to be exported.
        config_dict : dict
            Configuration dictionary, containing information about the data provider.
            The following keys and their values are expected when rendering the 
            jinja template:

            .. code-block:: Python

            dict(
                contact = dict(
                    organisationName = '',
                    deliveryPoint = '',
                    city = '',
                    administrativeArea = '',
                    postalCode = '',
                    country = '',
                    electronicMailAddress = ['', ''],
                    linkage = '',
                    linkage_name = '',
                    linkage_description = ''
                ),
                publisher = dict(
                    organisation_name = ''
                ))

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
        xml_etree : Union[ElementTree, None]
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
        xml_etree = entry.standards_export(config_dict=config_dict, template_path=template_path)

        if not path:
            return xml_etree

        # if path is given: write XML file
        else:
            # get the uuid of the ImmutableResultSet that is written to ISO19115 XML (rs.group.uuid or rs.get('uuid'))
            irs_uuid = ImmutableResultSet(entry).uuid

            # use absolute path
            path = os.path.abspath(path)

            # if path does not end with .xml: auto-generate XML filename
            if not path.endswith('.xml'):
                path += f"/iso19115_{irs_uuid}.xml"

            # write XML file
            with open(path, 'wb') as f:
                xml_etree.write(f, encoding='utf-8')


    @classmethod
    def cli_create_standards_xml(cls, args):
        """
        Function that determines which metadata standard to use and which 
        executes the appropriate cli_create_*_xml() function based on the 
        ``--format`` argument.

        .. versionadded:: 0.8.2
        
        """
        if args.format == 'iso19115':
            StandardsExportExtension.cli_create_iso19115_xml(args)


    @classmethod
    def cli_create_iso19115_xml(cls, args):
        """
        Adds functionality to the metacatalog CLI to enable ISO 19115
        XML export.
        Export one or more Entries, which are identified by positional
        argument entries. Entries can be identified by ID or UUID and  
        are exported in ISO 19115 format. The produced .xml file is saved 
        to the location specified with argument --path.
        If no path is given, the .xml file is saved to the current
        working directory.
        Use the flag --all to export all entries in the given metacatalog
        connection.

        .. versionadded:: 0.8.2

        Notes
        ----------
        The content of the xml files will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        a useful Metadata export.  

        """
        from metacatalog.api.catalog import create_iso19115_xml

        # get the session
        session = connect(args)

        # check path (mandatory)
        if args.path:
            path = args.path
        else:
            path = os.getcwd()

        # check not allowed combination of args
        if not args.entries and not args.all:
            cprint(args, "Please provide the ID(s) or UUID(s) of Entries to be exported or use flag --all to export all Entries in the database session.")
            exit(0)

        if args.entries and args.all:
            cprint(args, "Flag --all cannot be used together with an ID or UUID.")
            exit(0)

        # get entries to be exported
        if args.entries:
            # if '-' in x -> uuid, else id
            id_or_uuids = [str(x) if '-' in x else int(x) for x in args.entries]

        # flag --all: all entry ids
        elif args.all:
            id_or_uuids = [entry.id for entry in api.find_entry(session)]

        # run API ISO 19115 export function
        if args.verbose:
            for id_or_uuid in tqdm(id_or_uuids):
                create_iso19115_xml(session=session, id_or_uuid=id_or_uuid, config_dict={}, path=path)
        else:
            for id_or_uuid in id_or_uuids:
                create_iso19115_xml(session=session, id_or_uuid=id_or_uuid, config_dict={}, path=path)
