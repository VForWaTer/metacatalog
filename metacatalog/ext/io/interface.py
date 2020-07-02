from abc import abstractmethod

from .importer import import_to_internal_table, import_to_local_csv_file
from .reader import read_from_internal_table, read_from_local_csv_file
from .deleter import delete_from_internal_table, delete_from_local_csv
from .appender import append_to_internal_table, append_to_local_csv_file

from metacatalog.util.exceptions import IOOperationNotFoundError
from metacatalog.models import DataSource, Entry 
from metacatalog.ext import MetacatalogExtensionInterface


class IOExtensionInterface(MetacatalogExtensionInterface):
    """
    Absctract Base Class  for any kind of input / output 
    activity on all supported data sources. The Interface 
    can be used as an extension storage for read and write functions.
    For this, no interface has to be defined, as new functions
    can be registered and loaded by classmethods only. 

    To actually execute a read, import, delete or append operation,
    the interface needs to implement a calling function. That 
    means, you need to specify, how the function is called. Then, 
    an interface class can be defined for each Entry and data-source, 
    data-type and metadata-specific operations can be executed from 
    a common interface.
    """
    READER = dict(
        internal={
            'array': read_from_internal_table,
            'ndarray': read_from_internal_table
        },
        csv={
            'array': read_from_local_csv_file,
            'ndarray': read_from_local_csv_file
        }
    )
    IMPORTER = dict(
        internal={
            'array': import_to_internal_table,
            'ndarray': import_to_internal_table
        },
        csv={
            'array': import_to_local_csv_file,
            'ndarray': import_to_local_csv_file
        }
    )
    APPENDER = dict(
        internal={
            'iarray': append_to_internal_table,
            'timeseries': append_to_internal_table,
            'idataframe': append_to_internal_table,
            'time-dataframe': append_to_internal_table
        },
        csv={
            'iarray': append_to_local_csv_file,
            'timeseries': append_to_local_csv_file,
            'idataframe': append_to_local_csv_file,
            'time-dataframe': append_to_local_csv_file
        }
    )
    DELETER = dict(
        internal={
            'array': delete_from_internal_table,
            'ndarray': delete_from_internal_table
        },
        csv={
            'array': delete_from_local_csv,
            'ndarray': delete_from_local_csv
        }
    )
    def __init__(self, entry: Entry):
        """
        """
        self.init_new_entry(entry)

    def init_new_entry(self, entry: Entry):
        # Entry and Datasource instance
        self.entry = entry
#        self.datasource = entry.datasource
#        self.datatype = self.datasource.datatype
#
#        # SourceType and DataType names
#        self.sourcetype_name = self.datasource.type.name
#        self.datatype_name = self.datatype.name

    @abstractmethod
    def read(self, **kwargs):
        """
        Execute a read operation on the datasource.
        To load the registered function and run the after_read converter, 
        you can simply call the abstractmethod template from the new 
        Interface like:

        .. code-block:: python
            class IOInterface(IOExtensionInterface):
                def read(self, **kwargs):
                    return super(IOInterface, self).read(**kwargs)
        
        """
        # get reader
        reader = self.get_reader(self.entry.datasource)
        
        # build arguments
        args = self.entry.datasource.load_args()
        args.update(kwargs)
        
        # read the data
        data = reader(self.entry, self.entry.datasource, **kwargs)
        return self.after_read(data)
        
    def after_read(self, data):
        return data

    @abstractmethod
    def import_(self, data, **kwargs):
        """
        Execute an import operation on the datasource.
        To load the registered function and run the after_import converter, 
        you can simply call the abstractmethod template from the new 
        Interface like:

        .. code-block:: python
            class IOInterface(IOExtensionInterface):
                def import_(self, data, **kwargs):
                    return super(IOInterface, self).import_(data, **kwargs)
        
        """
        # get importer
        importer = self.get_importer(self.entry.datasource)
        
        # build arguments
        args = self.entry.datasource.load_args()
        args.update(kwargs)

        # import the data
        importer(self.entry, self.entry.datasource, data, **kwargs)
        return self.after_import()

    def after_import(self):
        pass
    
    @abstractmethod
    def append(self, data, **kwargs):
        """
        Execute an append operation on the datasource.
        To load the registered function and run the after_append converter, 
        you can simply call the abstractmethod template from the new 
        Interface like:

        .. code-block:: python
            class IOInterface(IOExtensionInterface):
                def append(self, data, **kwargs):
                    return super(IOInterface, self).append(data, **kwargs)

        """
        # get appender
        appender = self.get_appender(self.entry.datasource)

        # build arguments
        args = self.entry.datasource.load_args()
        args.update(kwargs)

        # append the data
        appender(self.entry, self.entry.datasource, data, **kwargs)
        return self.after_append()
        
    def after_append(self):
        pass

    @abstractmethod
    def delete(self, **kwargs):
        """
        Execute a delete operation on the datasource.
        To load the registered function and run the after_delete converter, 
        you can simply call the abstractmethod template from the new 
        Interface like:

        .. code-block:: python
            class IOInterface(IOExtensionInterface):
                def delete(self, **kwargs):
                    return super(IOInterface, self).delte(**kwargs)

        """
        # get deleter
        deleter = self.get_deleter(self.entry.datasource)

        # build arguments
        args = self.entry.datasource.load_args()
        args.update(kwargs)
        
        # delte the datasource
        deleter(self.entry, self.entry.datasource, **kwargs)
        return self.after_delete()

    def after_delete(self):
        pass

    @classmethod
    def register(cls, func: callable, operation: str, name: str, datatypes, overwrite=False):
        # make datatypes iterable
        if not isinstance(datatypes, (list, tuple)):
            datatypes = [datatypes]
        
        name = name.lower()
        op = operation.upper()

        try:
            D = getattr(cls, op)
        except AttributeError:
            raise AttributeError("'%s' is not a valid I/O operation" % op)
        
        if name not in D.keys():
            D[name] = dict()
        for datatype in datatypes:
            datatype = datatype.lower()
            if datatype in D[name].keys() and not overwrite:
                raise Warning("A '%s' %s for '%s' data-types already exists. use 'overwrite=True' to overwrite" % (name, operation, datatype))
            D[name][datatype] = func
        
        # set the new mapping
        setattr(cls, op, D)

    @classmethod
    def _get_types(cls, operation: str, name: str) -> dict:
        try:
            D = getattr(cls, operation)
        except AttributeError:
            raise AttributeError("'%s' is not a valid I/O operation" % operation.upper())
        
        try: 
            return D[name.lower()]
        except KeyError:
            raise AttributeError("The type '%s' is not registered for %s I/O operations" % (name.lower(), operation.upper()))

    @classmethod
    def _get_func(cls, operation: str, name: str, datatype: str) -> callable:
        types = cls._get_types(operation, name)

        if not datatype.lower() in types:
            raise IOOperationNotFoundError("No registered function for datatype '%s'\nOperation:[%s]->[%s]" % (datatype.lower(), operation.upper(), name.lower()))
        
        return types[datatype.lower()]

    @classmethod
    def get_func_for_datasource(cls, operation: str, datasource: DataSource) -> callable:
        """
        """
        op = operation.upper()
        if datasource.datatype is None or datasource.type is None:
            raise AttributeError('DataSources without DataType or DataSourceType cannot use I/O Operations.')
        else:
            datatype = datasource.datatype
            name = datasource.type.name
        
        # wrap the loader
        def load():
            try:
                return cls._get_func(operation, name, datatype.name)
            except IOOperationNotFoundError:
                return False
        
        # go for the function
        check_for_func = True
        while check_for_func:
            func = load()
            if func == False:
                if datatype.parent is not None:
                    datatype = datatype.parent
                else:
                    check_for_func = False
                    func = None
            else:
                check_for_func = False
        
        if func is None:
            raise IOOperationNotFoundError("No registered function for datatype '%s'\nOperation:[%s]->[%s]" % (datatypename, operation.upper(), name))

        return func

    @classmethod
    def get_reader(cls, datasource: DataSource):
        """
        Return the reader function of :class:`DataSource <metacatalog.models.DataSource>`, 
        do not use it directly.

        Parameters
        ----------
        datasource : DataSource
            The datasource instance that should use this function to 
            read data as specified.

        Returns
        -------
        reader : callable
            The reader function for the requested datasource type
        """
        return cls.get_func_for_datasource('READER', datasource)

    @classmethod
    def get_importer(cls, datasource: DataSource):
        """
        Return the importer function of :class:`DataSource <metacatalog.models.DataSource>`, 
        do not use it directly.

        Parameters
        ----------
        datasource : DataSource
            The datasource instance that should use this function to 
            import data as specified.

        Returns
        -------
        importer : callable
            The importer function for the requested datasource type
        """
        return cls.get_func_for_datasource('IMPORTER', datasource)

    @classmethod
    def get_appender(cls, datasource: DataSource):
        """
        Return the appender function of :class:`DataSource <metacatalog.models.DataSource>`, 
        do not use it directly.

        Parameters
        ----------
        datasource : DataSource
            The datasource instance that should use this function to 
            append data as specified.

        Returns
        -------
        appender : callable
            The appender function for the requested datasource type
        """
        return cls.get_func_for_datasource('APPENDER', datasource)
    
    @classmethod
    def get_deleter(cls, datasource: DataSource):
        """
        Return the deleter function of :class:`DataSource <metacatalog.models.DataSource>`, 
        do not use it directly.

        Parameters
        ----------
        datasource : DataSource
            The datasource instance that should use this function to 
            delete data as specified.

        Returns
        -------
        deleter : callable
            The deleter function for the requested datasource type
        """
        return cls.get_func_for_datasource('DELETER', datasource)

