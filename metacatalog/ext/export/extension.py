from typing import List
from datetime import datetime as dt
import json
import pickle
from dicttoxml import dicttoxml
import pandas as pd
import xarray

from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.models import Entry


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

class ExportExtension(MetacatalogExtensionInterface):
    r"""
    Export functions.

    The default ExportExtension can be used to produce raw export
    functionality. The :class:`Entry <metacatalog.models.Entry>` has
    a :func:`export <metacatalog.models.Entry.export>` function, that
    will translate the requested format to a activated extension of
    this name or fall back to this extension and call a function
    of same name.
    Raw export means, that the given Entry will be translated into a
    Python dictionary and then implemented into the given file-format
    as natively as possible. This does not follow any specified
    standards or rules. Standard metadata formats are implemented
    in separate extensions.

    Currently, the following exports are supported:

    * JSON
    * XML (called fast_XML)
    * pickle
    * netCDF

    .. note::
        The XML export is currently done with another package
        (``dicttoxml``). This works great, but it is not possible to
        adjust the exported XML to i.e. ISO19115 standard requirements.
        This will be implemented with an export via ``lxml``.
        Thus, for the base version of that export, the function ``xml``
        is reserved.

    If no path is specified, the native Python object that will be used
    to create the file is returned. This can be very useful to pack more
    than one Entry together. With ``path=None``, the export function will
    return the following objects:

    * JSON     -> str:    the JSON object as string
    * fast_XML -> str:    the XML representation as (decoded UTF-8) string
    * pickle   -> dict:   the dict underlying all export functions
    * netCDF   -> xarray: the xarray used to build the netCDF file

    Note that the export of ``'pickle'`` and ``'netCDF'`` can be particularly
    useful without setting the path.

    """
    @classmethod
    def init_extension(cls):
        pass
    
    @classmethod
    def to_dict(cls, entry: Entry, use_keys: List[str] = ENTRY_KEYS, serialize = True, no_data = False, clean = True, **kwargs) -> dict:
        """
        Return as dict to finally export.
        """
        # prevent circular import
        from metacatalog.util.results import ImmutableResultSet

        # get a resultSet of the entry
        result = ImmutableResultSet(entry)

        # output container
        out = dict()

        # add all keys
        for key in use_keys:
            # get the values
            val = result.get(key)

            # remove non
            if val is not None:
                if serialize:
                    out[key] = cls._serialize(val)
                else:
                    out[key] = val
        
        # handle data
        if not no_data:
            merged_df = pd.DataFrame()

            data = cls.get_data(entry, serialize=False, **kwargs)
            uncompressed = []

            for uuid, df in data.items():
                if not isinstance(df, pd.DataFrame):
                    uncompressed.append((uuid, df, ))
                    continue
                else:
                    merged_df = pd.merge(merged_df, df, left_index=True, right_index=True, how='outer')
        
            # handle output
            if not merged_df.empty:
                merged_df.reset_index(inplace=True)
                out['data'] = cls._serialize(merged_df.to_dict(orient='records'))

            # non-mergable data
            if len(uncompressed) > 0:
                out['unmerged_data'] = {k: cls._serialize(v) for k,v in uncompressed}
            
        # return
        if clean:
            out = cls._clear_output(out)

        return out

    @classmethod
    def get_data(cls, entry: Entry, serialize=True, **kwargs) -> dict:
        """
        Return the data as UUID indexed dict
        """
        # prevent circular import
        from metacatalog.util.results import ImmutableResultSet

        # get a resultSet of the entry
        result = ImmutableResultSet(entry)

        # # output container
        # out = dict()

        # # add data
        # for e in result._members:
        #     try:
        #         data = e.get_data(**kwargs)

        #         # handle strinify
        #         if serialize and hasattr(data, 'to_dict'):
        #             data = data.to_dict(orient='records')
                
        #         # append
        #         out[e.uuid] = data
        #     except Exception:
        #         # do nothing
        #         pass

        data = result.get_data(**kwargs)

        # apply serialize
        if serialize:
            out = dict()
            for uuid, df in data.items():
                if hasattr(df, 'to_dict'):
                    df = df.to_dict(orient='records')
                out[uuid] = df
            return out
        else:
            # return
            return data
    
    @classmethod
    def flat_keys(cls, data: dict, prefix = False, delimiter: str = '.', **kwargs) -> dict:
        """
        Turn nested dictionaries into flat dictionaries by expanding 
        nested keys using the given delimiter
        """
        tuples = []
        
        # expand the keys
        for key, value in data.items():
            # build the new key
            if prefix:
                prefixed_key = f'{prefix}{delimiter}{key}'
            else:
                prefixed_key = key
            
            # check value
            if isinstance(value, dict):
                tuples.extend(cls.flat_keys(value, prefixed_key, delimiter).items())
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    tuples.extend(cls.flat_keys({str(i): v}, prefixed_key, delimiter).items())
            else:
                tuples.append((prefixed_key, value))
        return dict(tuples)

    @classmethod
    def _serialize(cls, val):
        """
        Turn anything into a string that is not serializable.
        """
        if isinstance(val, (float, int, str)):
            return val
        elif isinstance(val, (list, float)):
            return [cls._serialize(v) for v in val]
        elif isinstance(val, dict):
            return cls._clear_output(val)
        elif isinstance(val, dt):
            return val.isoformat()
        else:
            return str(val)
    
    @classmethod
    def _clear_output(cls, data: dict, keep_ids=False) -> dict:
        """
        Stringify the output and remove all internal ids
        """
        return {k: cls._serialize(v) for k, v in data.items() if k != 'id' and not keep_ids}        

    @classmethod
    def json(cls, entry: Entry, path: str = None, flat = False, indent: int = 4, no_data = False, **kwargs):
        """
        Export an :class:`Entry <metacatalog.models.Entry>` to JSON.
        If a path is given, a new file will be created.

        Parameters
        ----------
        entry : metacatalog.models.Entry
            The entry instance to be exported
        path : str
            If given, a file location for export.
        flat : bool
            If True, the resulting JSON will be un-nested and build formerly
            nested keys like *parent.child*, where the delimiter defaults to
            '.' but can be changed.
            Defaults to False.
        indent : int
            The default indentation for the JSON file
        no_data : bool
            If set to True, the actual data will not be loaded and included.
            This can be helpful if the data is not serializable or very large.
        
        Returns
        -------
        out : str
            The JSON string if path is None, else None
        
        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        The list of exported properties is hardcoded into this extension, but can
        be overwritten. You can also import the list:

        >>> from metacatalog.ext.export.extension import ENTRY_KEYS

        A updated list can then be passed as kwargs:

        >>> use_keys = [k for k in ENTRY_KEYS if not k.startswith('embargo')]
        >>> Export = metacatalog.ext.extension('export')
        >>> Export.json(entry, '/temp/metadata.json', use_keys=use_keys)

        """
        # get the dict
        metadata = cls.to_dict(entry, no_data=no_data, **kwargs)

        # handle flat output
        if flat:
            metadata = cls.flat_keys(metadata, **kwargs)

        # check path settings
        if path is None:
            return json.dumps(metadata, indent=indent)
        else:
            with open(path, 'w') as f:
                json.dump(metadata, f, indent=indent)
    
    @classmethod
    def pickle(cls, entry: Entry, path=None, flat = False, serialize = False, no_data = False, **kwargs):
        """
        Export an :class:`Entry <metacatalog.models.Entry>` to Python dict.
        If a path is given, a new file will be created.

        Parameters
        ----------
        entry : metacatalog.models.Entry
            The entry instance to be exported
        path : str
            If given, a file location for export.
        flat : bool
            If True, the resulting JSON will be un-nested and build formerly
            nested keys like *parent.child*, where the delimiter defaults to
            '.' but can be changed.
            Defaults to False.
        serialize : bool
            If True, all output data will be converted to serializable types,
            if possible. This may not work for all data formats.
            If no path is given, it is recommended to set serializable to False.
            Defaults to False
        no_data : bool
            If set to True, the actual data will not be loaded and included.
            This can be helpful if the data is not serializable or very large.
        
        Returns
        -------
        out : dict
            The native Python dict if path is None, else None
        
        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        The list of exported properties is hardcoded into this extension, but can
        be overwritten. You can also import the list:

        >>> from metacatalog.ext.export.extension import ENTRY_KEYS

        A updated list can then be passed as kwargs:

        >>> use_keys = [k for k in ENTRY_KEYS if not k.startswith('embargo')]
        >>> Export = metacatalog.ext.extension('export')
        >>> Export.pickle(entry, '/temp/metadata.pickle', use_keys=use_keys)

        """
        # get the dict
        metadata = cls.to_dict(entry, serialize=serialize, no_data=no_data, **kwargs)

        # handle flat output
        if flat:
            metadata = cls.flat_keys(metadata, **kwargs)

        # check path settings
        if path is None:
            return metadata
        else:
            with open(path, 'wb') as f:
                pickle.dump(metadata, f)

    @classmethod
    def fast_xml(cls, entry: Entry, path=None, no_data = False, **kwargs):
        """
        Export an :class:`Entry <metacatalog.models.Entry>` to XML.
        If a path is given, a new file will be created. This is the fast
        XML version, which will convert the metadata to custom XML tags.

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
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        The list of exported properties is hardcoded into this extension, but can
        be overwritten. You can also import the list:

        >>> from metacatalog.ext.export.extension import ENTRY_KEYS

        A updated list can then be passed as kwargs:

        >>> use_keys = [k for k in ENTRY_KEYS if not k.startswith('embargo')]
        >>> Export = metacatalog.ext.extension('export')
        >>> Export.fast_xml(entry, '/temp/metadata.xml', use_keys=use_keys)

        """
        # get the dict
        metadata = cls.to_dict(entry, no_data=no_data, **kwargs)

        # convert
        xml = dicttoxml(metadata, custom_root='metadata', attr_type=False)
        
        # check path settings
        if path is None:
            return xml.decode()
        else:
            with open(path, 'wb') as f:
                f.write(xml)
    
    @classmethod
    def netcdf(cls, entry: Entry, path=None, **kwargs):
        """
        Export an :class:`Entry <metacatalog.models.Entry>` to netCDF or xarray.
        If a path is given, a new netCDF file will be created, if path is None,
        the xarray used for building the netCDF is returned.

        Note that the common attribute no_data, which is available for the
        other export functions, is not available for netCDF export.
        Furthermore, the flat flag is always true, as the Python netCDF
        implementation does not support nested attributes.

        Parameters
        ----------
        entry : metacatalog.models.Entry
            The entry instance to be exported
        path : str
            If given, a file location for export.

        Returns
        -------
        out : str
            The the XML str if path is None, else None
        
        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        The list of exported properties is hardcoded into this extension, but can
        be overwritten. You can also import the list:

        >>> from metacatalog.ext.export.extension import ENTRY_KEYS

        A updated list can then be passed as kwargs:

        >>> use_keys = [k for k in ENTRY_KEYS if not k.startswith('embargo')]
        >>> Export = metacatalog.ext.extension('export')
        >>> Export.netCDF(entry, '/temp/metadata.xml', use_keys=use_keys)

        """
        # get the metadata
        _m = cls.to_dict(entry, no_data=True, **kwargs)
        metadata = cls.flat_keys(_m)

        # get the data
        data = cls.get_data(entry, serialize=False)

        # container
        merged_df = pd.DataFrame()
        column_meta = {}

        # build up metadata
        for uuid, df in data.items():
            # can only work on DataFrame right now
            if not isinstance(df, pd.DataFrame):
                continue

            # merge the dataframes
            merged_df = pd.merge(merged_df, df, left_index=True, right_index=True, how='outer')
            
            # get the variable names and build their metadata
            names = df.columns
            for name in names:
                # add the UUID to reference attributes
                column_meta[name] = dict(uuid=uuid)
                for k, v in  metadata.items():
                    # in case more than one variable is referenced
                    if k.startswith(f'variable.{uuid}') or k.startswith(f'datasource.{uuid}'):
                        new_key = '.'.join(k.split('.')[2:])
                        column_meta[name][new_key] = v
                    
                    # in case only one variable contained - the elif is important here
                    elif k.startswith('variable.') or k.startswith('datasource.'):
                        column_meta[name][k] = v
            
            # build the xarray
            xr = xarray.Dataset.from_dataframe(merged_df)

            # add Dataset attributes
            xr.attrs = {k: v for k, v in metadata.items() if not k.startswith(f'variable.') and not k.startswith(f'datasource.')}

            # add column attributes
            for variable_name, attrs in column_meta.items():
                xr[variable_name].attrs = attrs

            # if no path is specified, return the xarray
            if path is None:
                return xr
            else:
                xr.to_netcdf(path)
