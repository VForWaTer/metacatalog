from typing import Union
from numpy import fromstring, full
from metacatalog.ext import MetacatalogExtensionInterface

from metacatalog import api

from metacatalog.models import Entry, Person
from metacatalog.util.results import ImmutableResultSet

from jinja2 import Environment, PackageLoader, FileSystemLoader
import pandas as pd
from sqlalchemy.orm.session import Session
from lxml import etree
import xmltodict

import shapely


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

def _init_iso19115_jinja(rs_dict):
        """
        Initialize jinja environment for ISO 19115 export.

        Parameters
        ----------
        rs_dict : dict
            ImmutableResultSet dictionary of exported entry
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
            As temporal resolution is stored as a string in metacatalog but ISO 19115
            XML requires resolution as type float, this function is used to return
            the temporal resolution in seconds.
            """
            resolution = pd.to_timedelta(resolution)
            return resolution.total_seconds()
        
        def datetime_to_date(datetime):
            """
            DateTime fields in metacatalog contain microseconds.
            For ISO export, we drop the time and just use the date.
            Use this filter to convert datetime to date and return in isoformat.
            """
            return datetime.date().isoformat()

        # register custom filter functions in jinja environment
        env.filters['temporal_resolution_to_seconds'] = temporal_resolution_to_seconds
        env.filters['datetime_to_date'] = datetime_to_date

        # get template
        template = env.get_template("iso19115-2.j2")
        
        return env, template


def _init_immutableResultSet_dict(entry_or_resultset: Union[Entry, ImmutableResultSet]) -> dict:
    """
    Loads the ImmutableResultSet of the input Entry (if not already an ImmutableResultSet) 
    and extracts the information necessary for ISO export from `ImmutableResultSet.to_dict()`.

    Not all attributes / relations of an Entry are included in `ImmutableResultSet.to_dict()`.
    The attributes required for ISO 19115 XML export are added to the dictionary with
    the help of this function.
    """
    if isinstance(entry_or_resultset, Entry):
        # get ImmutableResultSet
        rs = ImmutableResultSet(entry_or_resultset)
    else:
        rs = entry_or_resultset

    # get ImmutableResultSet dictionary
    rs_dict = rs.to_dict()

    # TODO: fileIdentifier


    # TODO: lastUpdate


    # TODO: title
    title = rs_dict.get('title')

    # TODO: publication


    # TODO: version


    # TODO: uuid (/fileIdentifier)


    # TODO: authors (last_name, first_name, organisation_name, role)


    # TODO: abstract


    # TODO: details_table


    # TODO: keywords (full_path, thesaurusName.title)


    # TODO: license (link, short_title)


    # TODO: associated_groups (uuid, type.name)


    # TODO: datasources (datasource.encoding, spatial_scale.resolution, spatial_scale.extent/bbox_location, temporal_scale.extent, temporal_scale.resolution, datasource.args)


    # XML field <gmd:IdentificationInfo> is repeatable -> put information of all entries in ImmutableResultSet here
    # list containing all dictionaries of entries in ImmutableResultSet
    # entry_dicts = []
    # for entry in rs._members:
    #     entry_dict = entry.to_dict()
    #     # include details table to put into abstract
    #     entry_dict['details_table'] = entry.details_table(fmt='md')

    #     # include associated groups
    #     entry_dict['associated_groups'] = entry.associated_groups

    #     # process location
    #     if 'datasource' in entry_dict:
    #         if 'spatial_scale' in entry_dict['datasource']:
    #             # get location from spatial_extent, spatial_scale is always a POLYGON, also for point locations
    #             location = entry_dict['datasource']['spatial_scale']['extent']
                
    #             # convert wkt to shapely shape to infer coordinates
    #             P = shapely.wkt.loads(location)
                
    #             # get support points of polygon
    #             min_lon, min_lat = P.exterior.coords[0][0], P.exterior.coords[0][1]
    #             max_lon, max_lat = P.exterior.coords[2][0], P.exterior.coords[2][1]
                
    #             # append to entry_dict
    #             entry_dict['bbox_location'] = {'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat}
    #     elif 'location' in entry_dict:
    #         # Entry.location is always a POINT
    #         location = entry_dict['location']
            
    #         # convert wkt to shapely shape to infer coordinates
    #         P = shapely.wkt.loads(location)
            
    #         # get coordinates of point
    #         min_lon = max_lon = P.coords[0][0]
    #         min_lat = max_lat = P.coords[0][1]

    #         # append to entry_dict
    #         entry_dict['bbox_location'] = {'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat}
    #     else:
    #         raise ValueError("No location associated with Entry.")

    #     # append entry_dict to list of entry_dicts
    #     entry_dicts.append(entry_dict)
    
    # # add entry_dicts to rs_dict
    # rs_dict['entry_dicts'] = entry_dicts

    # # ImmutableResultSet base group
    # rs_dict['base_group'] = rs.group

    # # ImmutableResultSet.to_dict() gives datetimes with milliseconds precision -> round to date, set
    # if isinstance(rs_dict['lastUpdate'], dict):
    #     rs_dict['lastUpdate_date'] = list(set([datetime.date() for datetime in rs_dict['lastUpdate'].values()]))
    # else:
    #     rs_dict['lastUpdate_date'] = [rs_dict['lastUpdate']]
    # # if entries in ImmutableResultSet have differing lastUpdate values: raise NotImplementedError (future idea: use basegroup?)
    # if len(rs_dict['lastUpdate_date']) == 1:
    #     rs_dict['lastUpdate_date'] = rs_dict['lastUpdate_date'][0]
    # else:
    #     raise NotImplementedError("Entries in ImmutableResultSet have differing dates for lastUpdate, export not possible yet.")


    return rs_dict


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
    def iso_xml_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict:dict, path: str=None, no_data=False, **kwargs):
        """
        Export a :class:`Entry <metacatalog.models.Entry>` or 
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet> to XML in ISO
        19115 standard.
        If a path is given, a new file will be created.

        Parameters
        ----------
        entry_or_resultset : Union[Entry, ImmutableResultSet]
            The entry instance to be exported
        config_dict : dict
            Configuration dictionary, containing information about the data provider
            - contact
        path : str
            If given, a file location for export.
        no_data : bool
            If set to True, the actual data will not be loaded and included.
            This can be helpful if the data is not serializable or very large.

        Returns
        -------
        out : str
            The XML str if path is None, else None

        Notes
        -----
        TODO: ImmutableResultSet or Entry??
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        a useful Metadata export.
        """
        # get ImmutableResultSet dictionary
        rs_dict = _init_immutableResultSet_dict(entry_or_resultset)

        # get initialized jinja environment and template
        env, template = _init_iso19115_jinja(rs_dict)

        # render template with entry_dict
        xml = template.render(**rs_dict, **config_dict)

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
    def iso_xml_import(xml, session, commit=True):
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
            
        commit : bool
            If commit is True, the Entry is directly created in the passed
            metacatalog session. If False, the metadata dictionary is returned,
            which can be checked, the Entry can then be created with 
            `Entry.from_dict()`.

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

        # author and coauthors
        authors = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:pointOfContact', [])

        coauthors_list = []

        for author in authors:
            role = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:role', {}).get('gmd:CI_RoleCode', {}).get('#text')
            if role == 'author':
                fullname = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:individualName', {}).get('gco:CharacterString')
                if fullname:
                    first_name = fullname.split(', ')[0]
                    last_name = fullname.split(', ')[1]
                else:
                    raise ValueError("No information about author first and last name provided.")
        
                organisation_name = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:organisationName', {}).get('gco:CharacterString')

                author_dict = dict(
                    first_name = first_name,
                    last_name = last_name,
                    organisation_name = organisation_name
                ) 
                author = Person.from_dict(author_dict)
            elif role == 'coAuthor':
                fullname = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:individualName', {}).get('gco:CharacterString')
                if fullname:
                    first_name = fullname.split(', ')[0]
                    last_name = fullname.split(', ')[1]
                else:
                    raise ValueError("No information about author first and last name provided.")

                organisation_name = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:organisationName', {}).get('gco:CharacterString')

                coauthor_dict = dict(
                    first_name = first_name,
                    last_name = last_name,
                    organisation_name = organisation_name,
                    role = role
                    #order=i -> not sure if we need an order
                ) 
                coauthors_list.append(coauthor_dict)

        # locationShape


        # location (currently only POINT shape implemented)
        west = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:westBoundLongitude', {}).get('gco:Decimal')
        east = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:eastBoundLongitude', {}).get('gco:Decimal')
        south = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:southBoundLatitude', {}).get('gco:Decimal')
        north = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:gmd:northBoundLatitude', {}).get('gco:Decimal')

        if west == east and south == north:
            location = (west, south)

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
        last_update = metadata.get('gmd:dateStamp', {}).get('gco:DateTime')

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


        # ADD ENTRY (by hand), we could also use Entry.from_dict()

        # add the entry
        entry = api.add_entry(
            session=session,
            title=title,
            author=author,
            location=location,
            variable=variable,
            abstract=abstract,
            external_id=external_id,
            geom=geom,
            license=license,
            embargo=embargo
        )


        d = dict(
            #id=id,
            uuid=uuid,
            title=title,
            author=author_dict(deep=False),
            authors=[a.to_dict(deep=False) for a in self.authors],
            locationShape=location_shape.wkt,
            location=location_shape.wkt,
            variable=variable.to_dict(deep=False),
            embargo=embargo,
            embargo_end=embargo_end,
            version=version,
            isPartial=is_partial,
            publication=publication,
            lastUpdate=lastUpdate,
            keywords=self.plain_keywords_dict()
            # datasource
        )


        # lazy loading
        if deep:
            projects = self.projects
            if len(projects) > 0:
                d['projects'] = [p.to_dict(deep=False) for p in projects]
            comp = self.composite_entries
            if len(comp) > 0:
                d['composite_entries'] = [e.to_dict(deep=False) for e in comp]
        # HIER FEHLEN AUCH LABELS UND SPLIT DATASETS!


        coauthors = [Person.from_dict(a, session) for a in coauthors_list]
        api.add_persons_to_entries(
            session,
            entries=[entry],
            persons=coauthors,
            roles=['coAuthor'] * len(coauthors),
            order=[_ + 2 for _ in range(len(coauthors))]
        )


        # USE Entry.from_dict() after the dictionary is built??
        if commit:
            Entry.from_dict(d)
        else:
            return d
    