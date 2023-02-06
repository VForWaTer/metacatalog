from typing import Union
from numpy import fromstring, full
from metacatalog.ext import MetacatalogExtensionInterface

from metacatalog import api

from metacatalog.models import Entry, Person
from metacatalog.util.results import ImmutableResultSet

from jinja2 import Environment, FileSystemLoader
import pandas as pd
from sqlalchemy.orm.session import Session
from lxml import etree
import xmltodict
import shapely
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKBElement

from datetime import datetime

import os

def _init_iso19115_jinja():
        """
        Initialize jinja environment for ISO 19115 export.

        """
        # folder containing ISO 19115 templates
        absolute_path = os.path.dirname(__file__)
        relative_path = "schemas/iso19115"
        full_path = os.path.join(absolute_path, relative_path)
        
        env = Environment(loader=FileSystemLoader(searchpath=full_path))
        
        # prevent whitespaces / newlines from jinja blocks in template
        env.trim_blocks = True
        env.lstrip_blocks = True

        # get template
        template = env.get_template("iso19115-2.j2")
        
        return template


def get_uuid(rs: ImmutableResultSet) -> str:
    """
    Returns uuid of ImmutableResultSet.  
    Returns the uuid of the base group (if exists) or the concatenated uuids of all 
    members.

    Returns
    ----------
    uuid : str
        Used for field <gmd:fileIdentifier> and field <gmd:identifier>, not repeatable.

    """
    # if a base group exists, use the uuid of the base group
    if rs.group:
        uuid = rs.group.uuid

    # if there is only one entry in the ImmutableResultSet, use its uuid
    elif isinstance(rs.get('uuid'), str):
        uuid = rs.get('uuid')

    # if there are more uuids in ImmutableResultSet, a list is returned, use latest
    elif isinstance(rs.get('uuid'), list):
        uuid = ''
        for i, _uuid in enumerate(rs.get('uuid')):
            uuid += f"uuid {i+1}: {_uuid}\n"

    return uuid


def get_lastUpdate(rs: ImmutableResultSet) -> str:
    """
    Returns lastUpdate of ImmutableResultSet.  
    Returns the uuid of the base group (if exists) or the latest value of lastUpdate of 
    all members.

    Returns
    ----------
    lastUpdate : str
        Used for field <gmd:dateStamp> and field <gmd:date> with <gmd:dateType> 'revision'
        and field <gmd:editionDate>, str in ISO-date-format, not repeatable.

    """
    # if a base group exists, use the title of the base group
    if rs.group:
        lastUpdate = rs.group.lastUpdate.date().isoformat()

    # if there is only one lastUpdate / entry in the ImmutableResultSet, use its lastUpdate
    elif isinstance(rs.get('lastUpdate'), datetime):
        lastUpdate = rs.get('lastUpdate').date().isoformat()

    # if there are more lastUpdates in ImmutableResultSet, a dict is returned, use latest
    elif isinstance(rs.get('lastUpdate'), dict):
        lastUpdate = max(rs.get('lastUpdate').values()).date().isoformat()

    return lastUpdate


def get_title(rs: ImmutableResultSet) -> str:
    """
    Returns title of ImmutableResultSet.  
    Returns the title of the base group (if exists) or the concatenated titles of all 
    members.

    Returns
    ----------
    title : str
        Used for field <gmd:title>, not repeatable.

    """
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
    return title


def get_publication(rs: ImmutableResultSet) -> str:
    """
    Returns publication date of ImmutableResultSet.  
    Returns the title of the base group (if exists) or the latest value of publication of 
    all members.

    Returns
    ----------
    publication : str
        Used for field <gmd:date> with <gmd:dateType> 'creation', 
        str in ISO-datae-formate, not repeatable.

    """
    # if a base group exists, use the publication date of the base group
    if rs.group:
        publication = rs.group.publication.date().isoformat()

    # if there is only one publication / entry in the ImmutableResultSet, use its publication
    elif isinstance(rs.get('publication'), datetime):
        publication = rs.get('publication').date().isoformat()

    # if there are more publications in ImmutableResultSet, a dict is returned, use latest
    elif isinstance(rs.get('publication'), dict):
        publication = max(rs.get('publication').values()).date().isoformat()

    return publication


def get_version(rs: ImmutableResultSet) -> int:
    """
    Returns version number of ImmutableResultSet.  
    Returns the maximal version number of all members.

    Returns
    ----------
    version: int
        Used for field <gmd:edition>, not repeatable.

    """
    # if there is only one version in the ImmutableResultSet, use it
    if isinstance(rs.get('version'), int):
        version = rs.get('version')

    # if there are more than one version in ImmutableResultSet, us latest
    elif isinstance(rs.get('version'), int):
        version = max(rs.get('version').values())

    return version


def get_authors(rs: ImmutableResultSet) -> list[dict]:
    """
    Returns all authors and coauthors of the ImmutableResultSet.

    Returns
    ----------
    authors: list[dict]
        Used for field <gmd:CI_ResponsibleParty>, list of dictionaries containing the 
        information about authors: mandatory keys are `first_name`, `last_name` and
        `organisation_name`, repeatable.

    """
    # rs.get('authors') gives the first author and all coAuthors
    # if there is only one entry in the ImmutableResultSet, a list of authors is returned by rs.get('authors')
    if isinstance(rs.get('authors'), list):
        authors = []
        for author_dict in rs.get('authors'):
            authors.append(
                {
                    'first_name': author_dict.get('first_name'),
                    'last_name': author_dict.get('last_name'),
                    'organisation_name': author_dict.get('organisation_name')
                    })
    # if there is more than one entry in the ImmutableResultSet, a entry_uuid-indexed dictionary of authors is returned
    elif isinstance(rs.get('authors'), dict):
        for entry_uuid, entry_authors in rs.get('authors').items():
            authors = []
            for author_dict in entry_authors:
                authors.append(
                    {
                    'first_name': author_dict['first_name'],
                    'last_name': author_dict['last_name'],
                    'organisation_name': author_dict['organisation_name']
                })
    
    return authors


def get_abstract(rs: ImmutableResultSet) -> str:
    """
    Returns abstract of ImmutableResultSet.
    If there is more than one abstract in the ImmutableResultSet, abstracts are
    concatenated.

    Returns
    ----------
    abstract: str
        Used for field <gmd:abstract>, not repeatable.

    """
    abstract = ''
    # if there is only one entry in the ImmutableResultSet, use its abstract
    if isinstance(rs.get('abstract'), str):
        abstract = rs.get('abstract')

    #  if there is more than one abstract in ImmutableResultSet, concatenate abstracts
    elif isinstance(rs.get('abstract'), dict):
        abstract = ''
        for i, _abstract in enumerate(rs.get('abstract').values()):
            abstract += f"Abstract {i+1}: {_abstract}\n"

    return abstract


def get_details(rs: ImmutableResultSet) -> list[str]:
    """
    Returns the details of the ImmutableResultSet.
    Details are currently written to XML as Markdown tables along the abstracts 
    of ImmutableResultSet members.

    Returns
    ----------
    details: list[str]
        Details as a markdown table, currently also in field <gmd:abstract>, not repeatable.

    """
    # create list with details_table for all entries in ImmutableResultSet
    details = []
    _details = {}

    # if there is only one entry in the ImmutableResultSet, a list details is returned by rs.get('authors')
    if isinstance(rs.get('details'), list):
        for detail_dict in rs.get('details'):
            # nested details
                if isinstance(detail_dict['value'], dict):
                    # include top-level detail of nested detail
                    _details[detail_dict['key']] = detail_dict.copy()
                    _details[detail_dict['key']]['value'] = 'nested'
                    
                    # remove unwanted key-value pairs
                    _details[detail_dict['key']] = {key: val for key, val in _details[detail_dict['key']].items() if key in ['value', 'key', 'entry_uuid', 'description']}

                    # go for nested details
                    for k, v in detail_dict['value'].items():
                        expand = {
                            f"{detail_dict['key']}.{k}": dict(
                            value=v,
                            key=detail_dict['key'],
                            entry_uuid=detail_dict['entry_uuid'],
                            description=detail_dict.get('description', 'nan')
                            )
                        }
                        _details.update(expand)
                # un-nested details
                else:
                    _details[detail_dict['key']] = detail_dict
                    # remove unwanted key-value pairs
                    _details[detail_dict['key']] = {key: val for key, val in _details[detail_dict['key']].items() if key in ['value', 'key', 'entry_uuid', 'description']}

        # turn into a transposed dataframe
        df = pd.DataFrame(_details).T

        # append markdown table to details
        details.append(df.to_markdown())

    # if there are more than one entries in the ImmutableResultSet, a entry_uuid-indexed dictionary of details is returned
    elif isinstance(rs.get('details'), dict):
        for entry_uuid, entry_details_list in rs.get('details').items():
            for detail_dict in entry_details_list:
                # nested details
                if isinstance(detail_dict['value'], dict):
                    # include top-level detail of nested detail
                    _details[detail_dict['key']] = detail_dict.copy()
                    _details[detail_dict['key']]['value'] = 'nested'
                    
                    # remove unwanted key-value pairs
                    _details[detail_dict['key']] = {key: val for key, val in _details[detail_dict['key']].items() if key in ['value', 'key', 'entry_uuid', 'description']}

                    # go for nested details
                    for k, v in detail_dict['value'].items():
                        expand = {
                            f"{detail_dict['key']}.{k}": dict(
                            value=v,
                            key=detail_dict['key'],
                            entry_uuid=detail_dict['entry_uuid'],
                            description=detail_dict.get('description', 'nan')
                            )
                        }
                        _details.update(expand)
                # un-nested details
                else:
                    _details[detail_dict['key']] = detail_dict
                    # remove unwanted key-value pairs
                    _details[detail_dict['key']] = {key: val for key, val in _details[detail_dict['key']].items() if key in ['value', 'key', 'entry_uuid', 'description']}

            # turn into a transposed dataframe
            df = pd.DataFrame(_details).T

            # append markdown table to details
            details.append(df.to_markdown())

    return details


def get_keywords(rs: ImmutableResultSet) -> list[dict]:
    """
    Returns the keywords of the ImmutableResultSet.
    If the variables of the ImmutableResultSet are linked to a thesaurus, the thesaurus
    keywords are also returned along the keywords that are linked to the ImmutableResultSet
    members.

    Returns
    ----------
    keywords: list[dict]
        Used for field <gmd:MD_Keywords>, list of dictionaries containing information about
        associated keywords, mandatory keys are `full_path` and `thesaurusName`.

    """
    keywords = []
    # go for keyword linked to variable first
    variable_dict = rs.get('variable')

    if 'keyword' in variable_dict:
        # get relevant information
        full_path = variable_dict.get('keyword').get('path')
        thesaurusName = variable_dict.get('keyword').get('thesaurusName').get('title')
        
        # append to keywords
        keywords.append({
            'full_path': full_path,
            'thesaurusName': thesaurusName
        })

    # TODO: test multiple keywords!
    # go for keywords linked directly to ImmutableResultSet next
    for keyword_dict in rs.get('keywords'):
        # get relevant information
        full_path = keyword_dict.get('path')
        thesaurusName = keyword_dict.get('thesaurusName').get('title')
        
        # append to keywords
        keywords.append({
            'full_path': full_path,
            'thesaurusName': thesaurusName
        })

    return keywords


def get_licenses(rs: ImmutableResultSet) -> list[dict]:
    """
    Returns the licenses of the ImmutableResultSet.

    Returns
    ----------
    licenses: list[dict]
        Used for field <gmd:resourceConstraints>, list of dictionaries containing information about
        associated licenses, mandatory keys are `link` and `short_title`.

    """
    licenses = []
    # if there is only one license in the ImmutableResultSet, there are no nested dicts
    if not any(isinstance(val, dict) for val in rs.get('license').values()):
        link = rs.get('license')['link']
        short_title = rs.get('license')['short_title']
        licenses.append({
            'link': link,
            'short_title': short_title
            })

    #  if there is more than one license in ImmutableResultSet, a uuid-indexed dict of licenses is returned, concatenate license information
    elif any(isinstance(val, dict) for val in rs.get('license').values()):
        link = ''
        short_title = ''
        for entry_uuid, license_dict in rs.get('license').items():
            link = license_dict['link']
            short_title = license_dict['short_title']
            licenses.append({
                'link': link,
                'short_title': short_title
            })

    return licenses


def get_datasource_information(rs: ImmutableResultSet) -> tuple[list[dict], list[dict], list[int]]:
    """
    Returns the temporal scales, the location as a bounding box and the spatial resolution 
    of the data of the ImmutableResultSet.

    Returns
    ----------
    temporal_scales: list[dict]
        Used for field <gmd:temporalElement>, list of dictionaries containing information
        about the temporal scale(s), mandatory keys are `temporal_extent_start`, 
        `temporal_extent_end` and `temporal_resolution`, repeatable.
    bbox_locations: list[dict]
        Used for field <gmd:geographicElement>, list of dictionaries containing the support
        points of the bounding box(es), mandatory keys are `min_lon`, `min_lat`, `max_lon`
        and `max_lat`, repeatable.
    spatial_resolutions: list[int]
        Used for field <gmd:spatialResolution>, list of integers [m], repeatable.

    """
    temporal_scales = []
    bbox_locations = []
    spatial_resolutions = []
    # datasource can be empty / no datasource associated
    if not rs.get('datasource'):
        pass
    
    # if there is only one datasource in the ImmutableResultSet, use its values
    elif not all(isinstance(val, dict) for val in rs.get('datasource').values()):
        # temporal_scale
        if 'temporal_scale' in rs.get('datasource').keys():
            # extent
            temporal_extent_start = rs.get('datasource')['temporal_scale']['extent'][0].isoformat()
            temporal_extent_end = rs.get('datasource')['temporal_scale']['extent'][1].isoformat()
            # resolution in seconds
            temporal_resolution = rs.get('datasource')['temporal_scale']['resolution']
            temporal_resolution = pd.to_timedelta(temporal_resolution).total_seconds()

            temporal_scales = [{
                "temporal_extent_start": temporal_extent_start,
                "temporal_extent_end": temporal_extent_end,
                "temporal_resolution": temporal_resolution
                }]

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
            bbox_locations = [{'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat}]

            # spatial_resolution
            spatial_resolutions = [rs.get('datasource')['spatial_scale']['resolution']]

    # if there are more than one datasources in the ImmutableResultSet, all values in rs.get('datasource') are uuid-indexed dicts
    elif all(isinstance(val, dict) for val in rs.get('datasource').values()):
        for i ,(entry_uuid, ds_dict) in enumerate(rs.get('datasource').items()):
            # temporal_scale
            if ds_dict.get('temporal_scale'):
                # extent
                temporal_extent_start = ds_dict['temporal_scale']['extent'][0].isoformat()
                temporal_extent_end = ds_dict['temporal_scale']['extent'][1].isoformat()
                # resolution in seconds
                temporal_resolution = ds_dict['temporal_scale']['resolution']
                temporal_resolution = pd.to_timedelta(temporal_resolution).total_seconds()

                temporal_scales.append({
                    "temporal_extent_start": temporal_extent_start,
                    "temporal_extent_end": temporal_extent_end,
                    "temporal_resolution": temporal_resolution
                })
            # spatial_scale / bbox_location & spatial_resolution
            if ds_dict.get('spatial_scale'):
                location = ds_dict['spatial_scale']['extent']
        
                # convert wkt to shapely shape to infer coordinates
                P = shapely.wkt.loads(location)
        
                # get support points of polygon
                min_lon, min_lat = P.exterior.coords[0][0], P.exterior.coords[0][1]
                max_lon, max_lat = P.exterior.coords[2][0], P.exterior.coords[2][1]
                
                # save as list(dict)
                bbox_locations.append({'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat})

                # spatial_resolution
                spatial_resolutions.append(ds_dict['spatial_scale']['resolution'])

    # if bbox_location is not filled from datasource above, go for Entry.location
    if not bbox_locations:
        if rs.get('location') and isinstance(rs.get('location'), WKBElement):
            # Entry.location is always a POINT
            location = rs.get('location')
            
            # convert wkt to shapely shape to infer coordinates
            P = to_shape(location)
            
            # get coordinates of point
            min_lon = max_lon = P.coords[0][0]
            min_lat = max_lat = P.coords[0][1]

            # save as bbox_location
            bbox_locations = [{'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat}]
        # more than one location -> uuid-indexed dict of locations
        if rs.get('location') and isinstance(rs.get('location'), dict):
            for entry_uuid, loc in rs.get('location').items():
                # Entry.location is always a POINT, save as bbox for ISO
                location = loc
            
                # convert wkt to shapely shape to infer coordinates
                P = to_shape(location)
            
                # get coordinates of point
                min_lon = max_lon = P.coords[0][0]
                min_lat = max_lat = P.coords[0][1]

                # append to bbox_location
                bbox_locations.append({'min_lon': min_lon, 'min_lat': min_lat, 'max_lon': max_lon, 'max_lat': max_lat})

    # raise ValueError if location is neither specified in datasource.spatial_scale nor in Entry.location
    if not bbox_locations:
        raise ValueError("No location information associated with instance to be exported.")

    return temporal_scales, bbox_locations, spatial_resolutions


def _parse_iso_information(entry_or_resultset: Union[Entry, ImmutableResultSet]) -> dict:
    """
    Loads the ImmutableResultSet of the input Entry (if not already an ImmutableResultSet) 
    and extracts the information necessary for ISO export.

    Parameters
    ----------
    entry_or_resultset : Union[Entry, ImmutableResultSet]
        The entry instance to be exported
    
    Returns
    ----------
    iso_input: dict
        Dictionary of information 

    """
    if not isinstance(entry_or_resultset, ImmutableResultSet):
        # get ImmutableResultSet
        rs = ImmutableResultSet(entry_or_resultset)
    else:
        rs = entry_or_resultset

    # uuid / fileIdentifier
    uuid = get_uuid(rs)

    # lastUpdate, convert to date, convert to isoformat
    lastUpdate = get_lastUpdate(rs)

    # title
    title = get_title(rs)

    # publication, convert to date, convert to isoformat
    publication = get_publication(rs)

    # version
    version = get_version(rs)

    # authors (last_name, first_name, organisation_name, role), always as list of dicts
    authors = get_authors(rs)

    # abstract
    abstract = get_abstract(rs)

    # details_table, put details into field <abstract> as markdown table for now
    details = get_details(rs)

    # keywords (full_path, thesaurusName.title)
    keywords = get_keywords(rs)

    ### licenses (link, short_title)
    licenses = get_licenses(rs)

    ### datasource (spatial_scale.resolution, spatial_scale.extent/bbox_location, temporal_scale.extent, temporal_scale.resolution)
    temporal_scales, bbox_locations, spatial_resolutions = get_datasource_information(rs)

    # save everything in dict
    iso_input = {
        'uuid': uuid, 'lastUpdate': lastUpdate, 'publication': publication, 'version': version, 'title': title, 
        'authors': authors, 'abstract': abstract, 'details': details, 'keywords': keywords, 'licenses': licenses,
        'temporal_scales': temporal_scales, 'bbox_locations': bbox_locations, 'spatials_resolutions': spatial_resolutions
        }

    return iso_input


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
    Currently, ISO 19115 export is implemented.
    """
    @classmethod
    def init_extension(cls):
        pass

    @classmethod
    def iso_xml_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict: dict, path: str = None):
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
            xml = etree.fromstring(xml) # this also checks whether xml is well-formed
            xml = etree.tostring(xml) # converts to binary string, necessary to write xml

            with open(path, 'w') as f:
                f.write(xml.decode(encoding='utf8'))


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
    