from metacatalog.models import Entry

import shutil
import os


def set_data(entry: Entry, data_path: str, **kwargs):
    """
    Set a netCDF file or folder of netCDFs as the data of an Entry.

    Parameters
    ----------
    entry : metacatalog.models.Entry
        Metacatalog entry for which the data is to be set.
    data_path : str
        Path to the netCDF file or folder of netCDFs you want to add as 
        the data of the entry ("source").
    datasource_path : str
        Datasource path of the entry.  
        Where the data is to be saved ("target").

    """
    # get the datasource path of the entry
    datasource_path = entry.datasource.path

    # use absolute paths
    data_path = os.path.abspath(data_path)
    datasource_path = os.path.abspath(datasource_path)

    # validate netcdf file / folder
    validate(data_path)

    # copy the data from data_path to the datasource path of the entry
    shutil.copy(data_path, datasource_path)

    return None


def get_data(entry: Entry, output_path: str, start, end, bbox, **kwargs):
    """
    Get the data of an Entry as a netCDF file or folder of netCDFs.

    Parameters
    ----------
    entry : metacatalog.models.Entry
        Metacatalog entry for which the data is loaded.
    output_path : str
        Path to the file or folder where the data is to be saved.

    """
    # get the datasource path of the entry
    datasource_path = entry.datasource.path

    # use absolute paths
    output_path = os.path.abspath(output_path)
    datasource_path = os.path.abspath(datasource_path)

    # filter data if start, end or bbox are given
    if start or end or bbox:
        datasource_path = filter_data(start, end, bbox)

    # copy the data from the datasource path of the entry to output_path
    shutil.copy(datasource_path, output_path)

    return None


def validate(data_path: str) -> bool:
    """
    Validate a netCDF file or folder of netCDFs.

    Parameters
    ----------
    data_path : str
        Path to the netCDF file or folder of netCDFs you want to validate.

    Returns
    -------
    valide: bool
        True if data_path is a netCDF file or folder of netCDFs consistent with
        the Entry's metadata, False otherwise.
    """
    raise NotImplementedError()

    # check if data_path is a netCDF file or folder of netCDFs
    # if not, raise ValueError
    # if yes, return True

    # check temporal and spatial scale? -> open with xarray and check?

    # check if data naming is consistent with the naming pattern saved to datasource.args?


def filter_data(start, end, bbox):
    """
    Filtering data based on temporal and spatial extent is based on
    file naming.  
    The naming pattern is saved to datasource.args under the key 
    `naming_pattern`. If no naming pattern is defined here, filtering
    is not possible.  
    Filtering is done by adjusting the datasource path to only include
    the data that is within the given temporal and spatial extent.

    """
    raise NotImplementedError()

    # check if naming pattern is defined in datasource.args
    # if not, raise ValueError

    # check if start, end, bbox are consistent with naming pattern
    # if not, raise ValueError

    # filter data based on naming pattern
    # if no data is left, raise ValueError

    # return datasource_path