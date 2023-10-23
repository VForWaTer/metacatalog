from sqlalchemy import event

from .interface import IOExtensionInterface
from metacatalog.models import Entry
from metacatalog.util.exceptions import MetadataMissingError
from. netcdf import set_data_netcdf, get_data_netcdf


class IOExtension(IOExtensionInterface):
    """
    Input / Output extension.
    This is the default extension for all kind of CRUD operations on the 
    actual data described by metacatalog. It can be used on two different 
    levels. As a class, it offers classmethods to add and return new 
    functions for performing the actual 
    TODO: finish documentation
    """
    @classmethod
    def init_extension(cls):
        """
        Add the IOExtension as an attribute to 
        the Entry model
        """
        # set the Extension interface
        Entry.io_extension = IOExtension

    @classmethod
    def set_data(cls, entry: Entry, data_path: str, **kwargs):
        """
        Set the data of an Entry.

        Parameters
        ----------
        entry : metacatalog.models.Entry
            Metacatalog entry for which the data is to be set.  
            The set_data function is chosen based on the datasource of the entry.
        data_path : str
            Path to the file or folder containing the data.

        """
        # get the datasource type and path
        if not entry.datasource:
            raise MetadataMissingError('No datasource information for the given Entry.')
        else:
            datasource_type = entry.datasource.type.name
            datasource_path = entry.datasource.path

        # set the data based on the datasource type
        if datasource_type == 'netcdf':
            set_data_netcdf(entry, data_path, **kwargs)

    @classmethod
    def get_data(cls, entry: Entry, output_path: str, **kwargs):
        """
        Get the data of an Entry.

        Parameters
        ----------
        entry : metacatalog.models.Entry
            Metacatalog entry for which the data is loaded.  
            The get_data function is chosen based on the datasource of the entry.
        output_path : str
            Path to the file or folder where the data is to be saved.
        
        """
        # get the datasource type and path
        if not entry.datasource:
            raise MetadataMissingError('No datasource information for the given Entry.')
        else:
            datasource_type = entry.datasource.type.name
            datasource_path = entry.datasource.path
        
        # get the data based on the datasource type
        if datasource_type == 'netcdf':
            get_data_netcdf(entry, output_path, **kwargs)