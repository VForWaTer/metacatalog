from metacatalog.models import Entry
from metacatalog.util.exceptions import MetadataMissingError

import requests
from datetime import datetime
import re
import os
import threddsclient


def set_data():
    """
    Not really possible for THREDDS server?  
    Correct datasource configuration in essential here.

    datasource.args:
        ['url']: url to the thredds server
        ['naming pattern']: lat, lon, date naming pattern
    
    """
    raise NotImplementedError('Setting data for THREDDS server is not possible.')


def get_data(entry: Entry, output_path: str, start: datetime=None, end: datetime=None, bbox=None, **kwargs):
    """
    Get data from THREDDS server.
    
    """
    # check data validity
    assert Entry.is_valid(entry)

    # use absolute paths
    output_path = os.path.abspath(output_path)

    # get URL of the dataset on the THREDDS server
    if entry.datasource.args.get('url', None): # TODO: we could also save this in datasource.path -> better!
        url = entry.datasource.args.get('url')
    else:
        raise MetadataMissingError('No URL to the THREDDS server for the given Entry saved in datasource.args.url')
    
    # get catalog of the dataset on the THREDDS server
    catalog = threddsclient.read_url(url)

    # get the name of the "highest-level dataset" in the catalog
    catalog_name = catalog.name

    # get all datasets in catalog
    datasets = catalog.flat_datasets()

    # filter if start, end or bbox are given
    if start or end or bbox:
        datasets = filter_data(datasets, entry, start, end, bbox)

    # threddsserver also gives the filesize -> we could implement a warning when users want to access very large files
    overall_file_size = 0 

    # create directory for dataset if it does not exist
    if not os.path.exists(f'{output_path}/{catalog.name}'):
        os.mkdir(f'{output_path}/{catalog.name}')

    # loop over datasets, download them and save them to output_path
    for ds in datasets:
        file_name = ds.name
        
        # increase overall file size
        overall_file_size += ds.bytes

        # get download url of dataset
        download_url = ds.download_url()

        response = requests.get(download_url)

        with open(f'{output_path}/{catalog.name}/{file_name}', 'wb') as f:
            f.write(response.content)
    
    return None


def validate() -> bool:
    raise NotImplementedError('As setting data for THREDDS server is not possible, validation is not possible either.')


def filter_data(datasets: list[threddsclient.nodes.DirectDataset], entry: Entry, url: str, start: datetime, end: datetime, bbox) -> list[threddsclient.nodes.DirectDataset]:
    """
    Filter data from THREDDS server.  
    Filtering is only based on file naming pattern.  
    If no naming pattern is defined in entry.datasource.args, filtering is not possible.
    Returns a list of threddsclient Datasets that match the given temporal and spatial extent.

    """
    if entry.datasource.args.get('naming_pattern', None):
        naming_pattern = entry.datasource.args.get('naming_pattern')
    else:
        raise MetadataMissingError('No naming pattern for the given Entry saved in datasource.args.naming_pattern, filtering is not possible')

    # Regular expression to match the naming pattern 
    # TODO: this only works for the given naming pattern, not for any naming pattern (like lat/lon) -> make this more general
    regex_pattern = naming_pattern.format(year='(\d{4})', month='(\d{2})', day='(\d{2})')
    regex = re.compile(regex_pattern)

    filtered_datasets = []

    # TODO: this only works for the given naming pattern, not for any naming pattern (like lat/lon) -> make this more general
    # filter datasets by start and end date
    for ds in datasets:
        file_name = ds.name
        # check if the file name matches the naming pattern
        match = regex.match(file_name)
        if match:
            year, month, day = match.groups()
            date = datetime(int(year), int(month), int(day))
            if start <= date <= end:
                filtered_datasets.append(ds)

    return filtered_datasets
    