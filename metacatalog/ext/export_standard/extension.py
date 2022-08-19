from numpy import fromstring, full
from metacatalog.ext import MetacatalogExtensionInterface

from metacatalog import api

from metacatalog.models import Entry

from jinja2 import Environment, PackageLoader, FileSystemLoader
import pandas as pd
from sqlalchemy.orm.session import Session
from lxml import etree
import xmltodict



# DEV not sure if this is a good place...
ENTRY_KEYS = (
    'uuid',
    'external_id',
    'title',
    'authors',
    'abstract',
    'citation',
    'location_shape',
    'variable',
    'license',
    'datasource',
    'details',
    'embargo',
    'embargo_end',
    'version',
    'latest_version',
    'plain_keyword_dict',
    'publication',
    'lastUpdate',
    'comment',
    'associated_groups'
)

import os
def _init_iso19115_jinja():
        """
        Initialize jinja envirnoment for ISO 19115 export.
        """
        # folder containing ISO 19115 templates
        absolute_path = os.path.dirname(__file__)
        relative_path = "schemas/iso19115"
        full_path = os.path.join(absolute_path, relative_path)
        
        env = Environment(loader=FileSystemLoader(searchpath=full_path))
        
        
        # prevent whitespaces / newlines from jinja blocks in template
        env.trim_blocks = True
        env.lstrip_blocks = True

        # custom jinja filter functions
        def temporal_resolution_to_seconds(resolution: str):
            """
            As temporal resolution is stored as a string in metacatalog but ISO 19915
            XML requires resolution as type float, this function can is used to return
            the temporal resolution in seconds.
            """
            resolution = pd.to_timedelta(resolution)
            return resolution.total_seconds()

        # register custom filter functions in jinja environment
        env.filters['temporal_resolution_to_seconds'] = temporal_resolution_to_seconds

        # get template inside folder
        template = env.get_template("iso19115-2.j2")

        return env, template


def _init_entry_dict(entry: Entry) -> dict:
    """
    Not all attributes / relations of an Entry are included in `Entry.to_dict(deep=True)`.
    The attributes required for ISO 19115 XML export are added to the dictionary with
    the help of this function.
    Includes `associated_groups`, `keywords` and `thesaurus`.
    """
    entry_dict = entry.to_dict(deep=True)

    # associated_groups
    # deep=True: also contains information about entries in groups
    entry_dict['associated_groups'] = [
        e.to_dict(deep=True) for e in entry.associated_groups]

    # keywords
    entry_dict['keywords'] = entry.keywords

    # thesaurus -> I think there is no link from Entry to the used thesaurus, currently only GCMD is implemented.
    session = Session.object_session(entry)
    entry_dict['thesaurus'] = api.find_thesaurus(session)[0].to_dict()

    return entry_dict


def _validate_xml(xml: str) -> bool:
    """
    Checks whether the input XML is well-formed (correct syntax).
    Currently, it is not checked whether the input XML is also valid (We could e.g.
    check with schematron for the 20 mandatory fields).
    """
    try: 
        etree.fromstring(xml)
    except etree.XMLSyntaxError as e:
        raise(e)

# name "ExportExtension"? -> than we need another extension for imports!
class StandardExportExtension(MetacatalogExtensionInterface):
    r"""
    Extension to export Entries in standard format.
    Currently, ISO 19115 export is implemented
    """
    @classmethod
    def init_extension(cls):
        pass

    @classmethod
    def iso_xml_export(cls, entry:Entry, path=None, no_data=False, **kwargs):
        """
        Export a :class:`Entry <metacatalog.models.Entry>` to XML in ISO
        19115 standard.
        If a path is given, a new file will be created.

        Parameters
        ----------
        entry : metacatalog.models.Entry
            The entry instance to be exported
        path : str
            If given, a file location for export.
        no_data : bool
            If set to True, the actual data will not be loaded and included.
            This can be helpful if the data is not serializable or very large.

        Returns
        -------
        out : str
            The the XML str if path is None, else None

        Notes
        -----
        TODO: ImmutableResultSet or Entry??
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        """
        # get initialized jinja environment and template
        env, template = _init_iso19115_jinja()

        # get Entry as dictionary
        entry_dict = _init_entry_dict(entry)

        # render template with entry_dict
        xml = template.render(**entry_dict)

        # check whether xml is well-formed
        _validate_xml(xml)

       # check path settings
        if path is None:
            #xml=etree.fromstring(xml)
            #xml=etree.tostring(xml) # binary string
            return xml
            
        else:
            xml = etree.fromstring(xml) # this also checks whether xml is well-formed
            xml = etree.tostring(xml) # converts to binary string, necessary to write xml
            with open(path, 'wb') as f:
                f.write(xml)


    @classmethod
    def iso_xml_import(xml, session):
        """
        Import an ISO 19115 XML file and convert to an Entry with the 
        metacatalog metadata model.
        If a metacatalog session is passed, the Entry is directly imported
        to the database.
        
        Parameters
        ----------
        xml : str
            Path to XML file holding a ISO 19115-2 metadata record.
            
        session : 
            
        no_data : 
            

        Returns
        -------
        out : 

        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        """
        # get xml metadata as dict
        with open(xml, "rb") as f:
            metadata = xmltodict.parse(f)

        # check metadata ISO 19115 compliance
        if metadata.keys().__str__() == "dict_keys(['gmi:MI_Metadata'])":
            metadata = metadata.get('gmi:MI_Metadata', {})
        elif metadata.keys().__str__() == "dict_keys(['gmi:MD_Metadata'])":
            metadata = metadata.get('gmi:MD_Metadata', {})
            raise Warning("ISO 19115-2 root element should be 'gmi:MI_Metadata', 'gmi:MD_Metadata' is also accepted but it is not guaranteed that the import will work.")
        else:
            raise ValueError("ISO 19115-2 root element must be either 'gmi:MI_Metadata' or 'gmi:MD_Metadata'")
        # we could also check <gmd:metadataStandardVersion>, but the root key is better for that I guess


        # uuid
        uuid = metadata.get('gmd:fileIdentifier', {}).get('gco:CharacterString')
        if api.find_entry(session, uuid=uuid):
            raise ValueError(f"The Entry with the uuid {uuid} already exists in the database session.")

        # title
        title = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:citation', {}).get('gmd:CI_Citation', {}).get('gmd:title', {}).get('gco:CharacterString')

        # author
        # evenutally if <gmd:role> == 'author'
        author_fullname = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:pointOfContact', {}).get('gmd:CI_ResponsibleParty', {}).get('gmd:individualName', {}).get('gco:CharacterString')
        if author_fullname:
            author_first_name = author_fullname.split(', ')[0]
            author_last_name = author_fullname.split(', ')[1]
        else:
            raise ValueError("No information about author first and last name provided.")
        author_organisation_name = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:pointOfContact', {}).get('gmd:CI_ResponsibleParty', {}).get('gmd:organisationName', {}).get('gco:CharacterString')

        author_dict = dict(
            first_name = author_first_name,
            last_name = author_last_name,
            organisation_name = author_organisation_name
        ) 

        # co-authors
        # TODO: add to template: CI_RoleCode: coAuthor instead of author!
        # evenutally if <gmd:role> == 'coAuthor' -> or first author is the author, next authors are co-authors?


        # locationShape


        # location


        # variable


        # embargo


        # embargo_end


        # version
        version = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:citation', {}).get('gmd:CI_Citation', {}).get('edition', {}).get('gco:CharacterString')

        # isPartial


        # publication
        publication = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:citation', {}).get('gmd:CI_Citation', {}).get('gmd:date', {}).get('gmd:CI_Date', {}).get('gmd:date', {}).get('gco:DateTime')
        # publication date also in <editionDate> 

        # lastUpdate


        # keywords
        

        # license
        license_short_title = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:resourceConstraints', {}).get('gmd:MD_Constraints', {}).get('gmd:useLimitation', {}).get('gco:CharacterString')
        # license url would also be available from template
        license = api.find_license(session, short_title=license_short_title)
        
        # details


        # datasource


        # abstract
        abstract = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:abstract', {}).get('gco:CharacterString')
        
        # external_id
        
        
        # comment
         
        
        # citation'


        # associated groups
        associated_groups = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:aggregationInfo', [])
        projects_labels = [] # need to find a way to differentiate projects from labels and composites from splits
        composites_splits = []
        for group in associated_groups:
            # project, label: <gmd:associationType == 'crossReference' -> not possible to identify EntryGroupType (project OR label!)
            if group.get('gmd:MD_AggregateInformation', {}).get('gmd:associationType', {}).get('gmd:DS_AssociationTypeCode', {}).get('#text') == 'largerWorkCitation':
                group_uuid = group.get('gmd:MD_AggregateInformation', {}).get('gmd:aggregateDataSetIdentifier', {}).get('gmd:RS_Identifier', {}).get('gmd:code', {}).get('gco:CharacterString')
                projects_labels.append(group_uuid)
            # composite, split dataset: <gmd:associationType> == 'largerWorkCitation' -> see above
            elif group.get('gmd:MD_AggregateInformation', {}).get('gmd:associationType', {}).get('gmd:DS_AssociationTypeCode', {}).get('#text') == 'crossReference':
                group_uuid = group.get('gmd:MD_AggregateInformation', {}).get('gmd:aggregateDataSetIdentifier', {}).get('gmd:RS_Identifier', {}).get('gmd:code', {}).get('gco:CharacterString')
                composites_splits.append(group_uuid)



        # base dictionary
        d = dict(
            id=self.id,
            uuid=self.uuid,
            title=self.title,
            author=self.author.to_dict(deep=False),
            authors=[a.to_dict(deep=False) for a in self.authors],
            locationShape=self.location_shape.wkt,
            location=self.location_shape.wkt,
            variable=self.variable.to_dict(deep=False),
            embargo=self.embargo,
            embargo_end=self.embargo_end,
            version=self.version,
            isPartial=self.is_partial,
            publication=self.publication,
            lastUpdate=self.lastUpdate,
            keywords=self.plain_keywords_dict()
        )

        # USE Entry.from_dict() after the dictionary is built??

        # lazy loading
        if deep:
            projects = self.projects
            if len(projects) > 0:
                d['projects'] = [p.to_dict(deep=False) for p in projects]
            comp = self.composite_entries
            if len(comp) > 0:
                d['composite_entries'] = [e.to_dict(deep=False) for e in comp]
        # HIER FEHLEN AUCH LABELS UND SPLIT DATASETS!

    