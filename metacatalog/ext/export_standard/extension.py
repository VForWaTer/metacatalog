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

from datetime import datetime

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
    if not isinstance(entry_or_resultset, ImmutableResultSet):
        # get ImmutableResultSet
        rs = ImmutableResultSet(entry_or_resultset)
    else:
        rs = entry_or_resultset

    # get ImmutableResultSet dictionary
    rs_dict = rs.to_dict()

    ### uuid / fileIdentifier
    # if a base group exists, use the uuid of the base group
    if rs.group:
        uuid = rs.group.uuid

    # if there is only one entry in the ImmutableResultSet, use its uuid
    elif isinstance(rs.get('uuid'), datetime):
        uuid = rs.get('uuid')

    # if there are more uuids in ImmutableResultSet, a list is returned, use latest
    elif isinstance(rs.get('uuid'), list):
        for i, _uuid in enumerate(rs.get('uuid')):
            uuid += f"uuid {i+1}: {_uuid}\n"


    ### lastUpdate, round to date, convert to isoformat
    # if a base group exists, use the title of the base group
    if rs.group:
        lastUpdate = rs.group.lastUpdate.date().isoformat()

    # if there is only one lastUpdate / entry in the ImmutableResultSet, use its lastUpdate
    elif isinstance(rs.get('lastUpdate'), datetime):
        lastUpdate = rs.get('lastUpdate').date().isoformat()

    # if there are more lastUpdates in ImmutableResultSet, a dict is returned, use latest
    elif isinstance(rs.get('lastUpdate'), dict):
        lastUpdate = max(rs.get('lastUpdate').values()).date().isoformat()


    ### title
    # if a base group exists, use the title of the base group
    if rs.group:
        title = rs.group.title

    # if there is only one title / entry in the ImmutableResultSet, use its title
    elif isinstance(rs.get('title'), str):
        title = rs.get('title')

    # if there are more titles in ImmutableResultSet, a dict is returned, concatenate titles
    elif isinstance(rs.get('title'), dict):
        title = ''
        for i, _title in enumerate(rs.get('title').values()):
            title += f"Title {i+1}: {_title}\n"
    # TODO: sort titles?? sort everything? (like uuid) -> uuid as 'index'?


    ### publication, round to date, convert to isoformat
    # if a base group exists, use the publication date of the base group
    if rs.group:
        publication = rs.group.publication.date().isoformat()

    # if there is only one publication / entry in the ImmutableResultSet, use its publication
    elif isinstance(rs.get('publication'), datetime):
        publication = rs.get('publication').date().isoformat()

    # if there are more publications in ImmutableResultSet, a dict is returned, use latest
    elif isinstance(rs.get('publication'), dict):
        publication = max(rs.get('publication').values()).date().isoformat()


    ### version
    # if there is only one version in the ImmutableResultSet, use it
    if isinstance(rs.get('version'), int):
        version = rs.get('version')

    # if there are more than one version in ImmutableResultSet, us latest
    elif isinstance(rs.get('version'), int):
        version = max(rs.get('version').values())


    ### uuid (/fileIdentifier)
    # if a base group exists, use the uuid of the base group
    if rs.group:
        uuid = rs.group.uuid

    # if there is only one uuid / entry in the ImmutableResultSet, use its uuid
    elif isinstance(rs.get('uuid'), str):
        uuid = rs.get('uuid')

    #  if there is more than one uuid in ImmutableResultSet, concatenate uuids
    elif isinstance(rs.get('uuid'), list):
        uuid = ''
        for i, _uuid in enumerate(rs.get('uuid')):
            uuid += f"UUID {i+1}: {_uuid}\n"


    ### authors (last_name, first_name, organisation_name, role), always as list of dicts
    # rs.get('authors') gives the first author and all coAuthors
    for entry_uuid, entry_authors in rs.get('authors').items():
        authors = []
        for entry_author in entry_authors:
            authors.append(
                {
                'entry_uuid': entry_uuid, # use entry_uuid as 'index' to connect authors to entry
                'first_name': entry_author['first_name'],
                'last_name': entry_author['last_name'],
                'organisation_name': entry_author['organisation_name']
                #'role': 'XYZ' # TODO: connect role to persons?, for now always 'author' in ISO template
            })


    ### abstract
    # if there is only one entry in the ImmutableResultSet, use its abstract
    if isinstance(rs.get('abstract'), str):
        abstract = rs.get('abstract')

    #  if there is more than one abstract in ImmutableResultSet, concatenate abstracts
    elif isinstance(rs.get('abstract'), dict):
        abstract = ''
        for i, _abstract in enumerate(rs.get('abstract').values()):
            abstract += f"Abstract {i+1}: {_abstract}\n"

    
    ### details_table, put details into field <abstract> as markdown table for now
    # create list with details_table for all entries in ImmutableResultSet
    details = []

    for entry_uuid, entry_details_list in rs.get('details').items():
        _details = {}
        for detail in entry_details_list:
            # nested details
            if isinstance(detail['value'], dict):
                # include top-level detail of nested detail
                _details[detail['key']] = detail.copy()
                _details[detail['key']]['value'] = 'nested'
                
                # remove unwanted key-value pairs
                _details[detail['key']] = {key: val for key, val in _details[detail['key']].items() if key in ['value', 'key', 'entry_uuid', 'description']}

                # go for nested details
                for k, v in detail['value'].items():
                    expand = {
                        f"{detail['key']}.{k}": dict(
                        value=v,
                        #id=detail['id'],
                        key=detail['key'],
                        #stem=detail['stem'],
                        #entry_id=detail['entry_id'],
                        entry_uuid=detail['entry_uuid'],
                        description=detail.get('description', 'nan') #TODO: description is never included in Entry.details_table()!
                        )
                    }
                    _details.update(expand)
            # un-nested details
            else:
                _details[detail['key']] = detail
                # remove unwanted key-value pairs
                _details[detail['key']] = {key: val for key, val in _details[detail['key']].items() if key in ['value', 'key', 'entry_uuid', 'description']}

        # turn into a transposed datarame
        df = pd.DataFrame(_details).T

        # append markdown table to details
        details.append(df.to_markdown())


    # TODO: keywords (full_path, thesaurusName.title)


    ### license (link, short_title)
    # if there is only one license in the ImmutableResultSet, there are no nested dicts
    if not any(isinstance(val, dict) for val in rs.get('license').values()):
        link = rs.get('license')['link']
        short_title = rs.get('license')['short_title']

    #  if there is more than one license in ImmutableResultSet, a uuid-indexed dict of licenses is returned, concatenate license information
    elif any(isinstance(val, dict) for val in rs.get('license').values()):
        link = ''
        short_title = ''
        for i, (entry_uuid, license_dict) in enumerate(rs.get('license').items()):
            link += f"License {i+1}: {license_dict['link']}\n"
            short_title += f"License {i+1}: {license_dict['short_title']}\n"


    ### datasource (datasource.encoding, spatial_scale.resolution, spatial_scale.extent/bbox_location, temporal_scale.extent, temporal_scale.resolution, datasource.args)
    # datasource can be empty / no datasource associated
    if not rs.get('datasource'):
        pass


    # TODO: encoding auch bei IdentificationInfo immer utf-8?? denke schon
    

    # if there is only one datasource in the ImmutableResultSet, use its values
    elif not any(isinstance(val, dict) for val in rs.get('datasource').values()):
        # encoding
        encoding = rs.get('datasource')['encoding']

        # temporal_scale
        if 'temporal_scale' in rs.get('datasource').keys():
            # extent
            temporal_extent_start = rs.get('datasource')['temporal_scale']['extent'][0].isoformat()
            temporal_extent_end = rs.get('datasource')['temporal_scale']['extent'][1].isoformat()
            # resolution in seconds
            temporal_resolution = rs.get('datasource')['temporal_scale']['resolution']
            temporal_resolution = pd.to_timedelta(temporal_resolution)

        # spatial extent, always as a bounding box
        # go for spatial_scale in datasource first
        if 'spatial_scale' in rs.get('datasource').keys():
            location = rs.get('datasource')['spatial_scale']['extent']
            
            # convert wkt to shapely shape to infer coordinates
            P = shapely.wkt.loads(location)
            
            # get support points of polygon
            min_lon, min_lat = P.exterior.coords[0][0], P.exterior.coords[0][1]
            max_lon, max_lat = P.exterior.coords[2][0], P.exterior.coords[2][1]
            
            # save as list(dict)
            bbox_location = [{'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat}]

    # if there is only one datasource in the ImmutableResultSet, use its values
    elif any(isinstance(val, dict) for val in rs.get('datasource').values()):
        encoding = []
        for i ,(entry_uuid, ds_dict) in enumerate(rs.get('datasource').items()):
            # encoding
            encoding.append(ds_dict['encoding'])

        # check encoding -> TODO: ist das nicht eh immer utf-8?
        if len(set(encoding)) == 1:
            encoding = encoding[0]
        else:
            NotImplementedError("I think we don't need that..")


    # TODO: location
    # if bbox_location is not filled from datasource above, go for Entry.location
    if not bbox_location:
        rs.get('location')

    # raise ValueError if location is neither specified in datasource.spatial_scale nor in Entry.location
    if not bbox_location:
        raise ValueError("No location information associated with instance to be exported.")







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
    