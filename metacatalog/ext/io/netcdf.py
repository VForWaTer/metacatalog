from metacatalog.models import Entry


def set_data_netcdf(entry: Entry, data_path: str, **kwargs):
    """
    Set a netCDF file or folder of netCDFs as the data of an Entry.

    Parameters
    ----------
    entry : metacatalog.models.Entry
        Metacatalog entry for which the data is to be set.
    data_path : str
        Path to the netCDF file or folder of netCDFs.

    """
    pass


def get_data_netcdf(entry: Entry, output_path: str, **kwargs):
    """
    Get the data of an Entry as a netCDF file or folder of netCDFs.

    Parameters
    ----------
    entry : metacatalog.models.Entry
        Metacatalog entry for which the data is loaded.
    output_path : str
        Path to the file or folder where the data is to be saved.

    """
    pass