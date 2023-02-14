from typing import Union
import os
from datetime import datetime


from jinja2 import Environment, FileSystemLoader, Template
import pandas as pd
from lxml import etree
import shapely
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKBElement
import xml.etree.ElementTree as ET


from metacatalog.models import Entry
from metacatalog.util.results import ImmutableResultSet


def _init_jinja(template_path: str) -> Template:
        """
        Initialize jinja environment for metadata export
        and return the template.

        Parameters
        ----------
        template_path : str
            Location of the jinja2 template for metadata export.  
            Currently defaults to the ISO 19115 template.

        Returns
        ----------
        template : jinja2.environment.Template
            The jinja2 template object which will be rendered.

        """
        # get absolute path of template
        template_path = os.path.abspath(template_path)
        
        # get the template name
        template_name = os.path.basename(template_path)

        # get the template directory
        template_dir = os.path.dirname(template_path)

        # initialite environment
        env = Environment(loader=FileSystemLoader(searchpath=template_dir))
        
        # prevent whitespaces / newlines from jinja blocks in template
        env.trim_blocks = True
        env.lstrip_blocks = True

        # get template
        template = env.get_template(template_name)
        
        return template


def _get_uuid(rs: ImmutableResultSet) -> str:
    """
    Returns uuid of ImmutableResultSet.  
    Returns the uuid of the base group (if exists). If no base group
    exists in the ImmutableResultSet, the uuid of the (single) member 
    is returned.

    Returns
    ----------
    uuid : str
        Used for field <gmd:fileIdentifier> and field <gmd:identifier>, not repeatable.

    """
    return rs.uuid


def _get_lastUpdate(rs: ImmutableResultSet) -> str:
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


def _get_title(rs: ImmutableResultSet) -> str:
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


def _get_publication(rs: ImmutableResultSet) -> str:
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


def _get_version(rs: ImmutableResultSet) -> int:
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


def _get_authors(rs: ImmutableResultSet) -> list[dict]:
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


def _get_abstract(rs: ImmutableResultSet) -> str:
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


def _get_details(rs: ImmutableResultSet) -> list[str]:
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


def _get_keywords(rs: ImmutableResultSet) -> list[dict]:
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

    # one variable in ImmutableResultSet -> variable_dict is returned directly
    if not all(isinstance(val, dict) for val in variable_dict.values()):
        if 'keyword' in variable_dict:
            # get relevant information
            full_path = variable_dict.get('keyword').get('path')
            thesaurusName = variable_dict.get('keyword').get('thesaurusName').get('title')
            
            # append to keywords
            keywords.append({
                'full_path': full_path,
                'thesaurusName': thesaurusName
            })

    # more than one variable in ImmutableResultSet -> uuid-indexed dictionaries -> all values are dicts
    elif all(isinstance(val, dict) for val in variable_dict.values()):
        for entry_uuid, variable_dict in variable_dict.items():
            if 'keyword' in variable_dict:
                # get relevant information
                full_path = variable_dict.get('keyword').get('path')
                thesaurusName = variable_dict.get('keyword').get('thesaurusName').get('title')
                
                # append to keywords
                keywords.append({
                    'full_path': full_path,
                    'thesaurusName': thesaurusName
                })

    # go for keywords linked directly to ImmutableResultSet next
    # only one member in ImmutableResultSet: rs.get('keywords') returns a list of keyword dictionaries
    if isinstance(rs.get('keywords'), list):
        for keyword_dict in rs.get('keywords'):
            # get relevant information
            full_path = keyword_dict.get('path')
            thesaurusName = keyword_dict.get('thesaurusName').get('title')
            
            # append to keywords
            keywords.append({
                'full_path': full_path,
                'thesaurusName': thesaurusName
            })
    # more than one member in ImmutableResultSet: uuid-indexed dictionary with list of keyword_dicts as values
    elif isinstance(rs.get('keywords'), dict):
        for entry_uuid, keywords_list in rs.get('keywords').items():
            for keyword_dict in keywords_list:
                # get relevant information
                full_path = keyword_dict.get('path')
                thesaurusName = keyword_dict.get('thesaurusName').get('title')
                
                # append to keywords
                keywords.append({
                    'full_path': full_path,
                    'thesaurusName': thesaurusName
                })

    return keywords


def _get_licenses(rs: ImmutableResultSet) -> list[dict]:
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

    #  if there is more than one license in ImmutableResultSet, a uuid-indexed dict of licenses is returned
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


def _get_datasource_information(rs: ImmutableResultSet) -> tuple[list[dict], list[dict], list[int]]:
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
    uuid = _get_uuid(rs)

    # lastUpdate, convert to date, convert to isoformat
    lastUpdate = _get_lastUpdate(rs)

    # title
    title = _get_title(rs)

    # publication, convert to date, convert to isoformat
    publication = _get_publication(rs)

    # version
    version = _get_version(rs)

    # authors (last_name, first_name, organisation_name, role), always as list of dicts
    authors = _get_authors(rs)

    # abstract
    abstract = _get_abstract(rs)

    # details_table, put details into field <abstract> as markdown table for now
    details = _get_details(rs)

    # keywords (full_path, thesaurusName.title)
    keywords = _get_keywords(rs)

    ### licenses (link, short_title)
    licenses = _get_licenses(rs)

    ### datasource (spatial_scale.resolution, spatial_scale.extent/bbox_location, temporal_scale.extent, temporal_scale.resolution)
    temporal_scales, bbox_locations, spatial_resolutions = _get_datasource_information(rs)

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
    Currently, it is not checked whether the input XML is also a 
    valid ISO19115 XML.
    If the XML is well-formed, this function returns True.

    """
    try: 
        etree.fromstring(xml)
        return True
    except etree.XMLSyntaxError as e:
        raise(e)