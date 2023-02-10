from typing import Union


import os
from tqdm import tqdm
from sqlalchemy.orm import Session


from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.ext.standard_export.util import _parse_iso_information, _init_iso19115_jinja, _validate_xml, _get_uuid
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
        def wrapper_entry(self: Entry, config_dict: dict, path: str = None):  
            return StandardsExportExtension.iso19115_export(entry_or_resultset=self, config_dict=config_dict, path=path)
        
        # iso19115_export docstring and name for wrapper function
        wrapper_entry.__doc__ = StandardsExportExtension.iso19115_export.__doc__
        wrapper_entry.__name__ = StandardsExportExtension.iso19115_export.__name__
        
        # add wrapper to Entry model
        Entry.export_iso19115 = wrapper_entry

        # add function create_iso19115 to api.catalog
        def wrapper_api(session: Session, config_dict: dict, path: str, if_exists: str = 'fail', verbose: bool = False):
            StandardsExportExtension.create_iso19115(session, config_dict, path, if_exists, verbose)

        wrapper_api.__doc__ = StandardsExportExtension.create_iso19115.__doc__
        wrapper_api.__name__ = StandardsExportExtension.create_iso19115.__name__

        # add wrapper to api.catalog
        api.catalog.create_iso19115 = wrapper_api


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


    def create_iso19115(session: Session, config_dict: dict, path: str, if_exists: str = 'fail', verbose: bool = False) -> None:
        """
        Generate ISO 19115 XML files for all ImmutableResultSets in the
        database session. The XML files are saved in the folder given in
        ``path``, existing files in the folder are deleted, so use this 
        function with caution.

        .. versionadded:: 0.7.4

        Parameters
        ----------
        session : sqlalchemy.Session
            SQLAlchemy session connected to the database.
        config_dict : dict
            Configuration dictionary, containing information about the data provider
        path : str
            Folder location where all ISO19115 XML files are saved to.
        if_exists: {'fail', 'replace'}, default 'fail'
            How to behave if the XML file for the ImmutableResultSet already exists in path.

            * fail: Raise a ValueError
            * replace: Overwrite the existing XML file.
        verbose: bool, default False
            Enable verbose output.        

        """
        from metacatalog.ext.standard_export.util import _get_uuid
        from tqdm import tqdm

        if if_exists not in ("fail", "replace"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        irs_uuids = []

        # use absolute path
        path = os.path.abspath(path)

        # list files to check if a file already exists
        files = os.listdir(path)

        # create the generator 
        if verbose:
            gen = tqdm(api.find_entry(session))
        else:
            gen = api.find_entry(session)

        for entry in gen:
            # get the uuid of the ImmutableResultSet that is written to ISO19115 XML (rs.group.uuid or rs.get('uuid'))
            irs_uuid = _get_uuid(ImmutableResultSet(entry))

            # if irs_uuid in irs_uuids: ImmutableResultSet already exported -> continue
            if irs_uuid in irs_uuids:
                continue
            else:
                # check if_exists policy first
                if any(irs_uuid in file for file in files):
                    if if_exists == 'fail':
                        raise ValueError(f"ISO19115 XML file for uuid '{irs_uuid}' already exists under {path}.")
                
                entry.export_iso19115(config_dict, path=f"{path}/iso19115_{irs_uuid}.xml")

                irs_uuids.append(irs_uuid)

