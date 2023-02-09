from typing import Union


from lxml import etree
import xmltodict


from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.ext.standard_export.util import _parse_iso_information, _init_iso19115_jinja, _validate_xml
from metacatalog import api
from metacatalog.models import Entry, Person
from metacatalog.util.results import ImmutableResultSet


class StandardsExportExtension(MetacatalogExtensionInterface):
    r"""
    Extension to export Entries in standard format.
    Currently, ISO 19115 export is implemented.
    """
    @classmethod
    def init_extension(cls):
        # wrapper which calls StandardsExportExtension.iso19115_export
        def wrapper(self: Entry, config_dict: dict, path: str = None):  
            return StandardsExportExtension.iso19115_export(entry_or_resultset=self, config_dict=config_dict, path=path)
        
        # iso19115_export docstring and name for wrapper function
        wrapper.__doc__ = StandardsExportExtension.iso19115_export.__doc__
        wrapper.__name__ = StandardsExportExtension.iso19115_export.__name__
        
        Entry.export_iso19115 = wrapper


    @classmethod
    def iso19115_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict: dict, path: str = None):
        """
        Export a :class:`Entry <metacatalog.models.Entry>` or 
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet> to XML in 
        ISO 19115 standard.
        Repeatable information input is always a list, as we can loop over the lists in the
        jinja ISO 19115 template.
        If a path is given, a new XML file will be created.

        Parameters
        ----------
        entry_or_resultset : Union[Entry, ImmutableResultSet]
            The entry instance to be exported
        config_dict : dict
            Configuration dictionary, containing information about the data provider
            - contact
        path : str
            If given, a file location for export.
        
        Returns
        ----------
        out : str
            The XML str if path is None, else None

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
        _validate_xml(xml)

        # check path settings
        if path is None:
            return xml
            
        else:
            with open(path, 'w') as f:
                f.write(xml)
