from typing import List
from datetime import datetime as dt
import json
import pickle

from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.models import Entry


# DEV not sure if this is a good palce...
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
    @classmethod
    def init_extension(cls):
        pass
    
    @classmethod
    def to_dict(cls, entry: Entry, use_keys: List[str] = ENTRY_KEYS, serialize = True, no_data = False, **kwargs) -> dict:
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
            data = cls.get_data(entry, **kwargs)

            # handle json output
            json_data = [cls._serialize(d) for d in data]

            out['data'] = json_data
        
        # return
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

        # output container
        out = dict()

        # add data
        for e in result._members:
            try:
                data = e.get_data(**kwargs)

                # handle strinify
                if serialize and hasattr(data, 'to_dict'):
                    data = data.to_dict(orient='records')
                
                # append
                out[e.uuid] = data
            except Exception:
                # do nothing
                pass
        
        # return
        return out
    
    @classmethod
    def flat_keys(cls, data: dict, delimiter: str = '.', **kwargs) -> dict:
        """
        Turn nested dictionaries into flat dictionaries by expanding 
        nested keys using the given delimiter
        """
        out = dict()

        # expand the keys
        for key, value in data.items():
            if isinstance(value, dict):
                nested = cls._flatten(key, value, delimiter, '')
                out.update(nested)
            else:
                out[key] = value
        
        # return 
        return out

    @classmethod
    def _flatten(cls, key, value, delimiter, prefix):
        if isinstance(value, dict):
            prefixed_key = ''.join([prefix, key, delimiter])
            return {f'{prefixed_key}{k}': cls._flatten(v, k, delimiter, prefix=prefixed_key) for k, v in value.items()}
        else:
            return value

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
            return cls.clear_output(val)
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
        
        Retruns
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

        >> from metacatalog.ext.export.extension import ENTRY_KEYS

        A updated list can then be passed as kwargs:

        >> use_keys = [k for k in ENTRY_KEYS if not k.startswith('embargo')]
        >> Export = metacatalog.ext.extension('export')
        >> Export.json(entry, '/temp/metadata.json', use_keys=use_keys)

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
        
        Retruns
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

        >> from metacatalog.ext.export.extension import ENTRY_KEYS

        A updated list can then be passed as kwargs:

        >> use_keys = [k for k in ENTRY_KEYS if not k.startswith('embargo')]
        >> Export = metacatalog.ext.extension('export')
        >> Export.pickle(entry, '/temp/metadata.pickle', use_keys=use_keys)

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
