"""
The ImmutableResultSet class can be used to wrap a list of
:class:`Entry <metacatalog.models.Entry>` or 
:class:`EntryGroup <metacatalog.models.EntryGroup>` instances
a deliver a unified interface to load metadata from nested 
and / or grouped results.

Example
~~~~~~~

>>> session = api.connect_database()
>>> entry = api.find_entry(session, id=42)
>>> res = ImmutableResultSet(entry)

This will load all sibling Entries into the result set. I.e., if
the composite entry has three members, the Composite EntryGroup 
and two child Entries, the ImmutableResultSet will have three
UUIDs

>>> res.get('uuid')


>>    ['c1087040-5566-44b9-9852-ea600f73ae0c',
>>    '0350c985-4e43-4876-a03a-7b6fb2a3a4b6',
>>    'c6d6301b-8e1c-478f-9407-ce9a0c38dbb8']

At the same time, if all members share a property, like their ``title``
the ImmutableResultSet will merge matching content:

>>> res.get('title')

>>    'Awesome Composite Dataset'

Note that the return type is string, like the original ``title``,
not a list.

Alternatively, the API can return ImmutableResultSets if the
API has a ``as_result`` arugment. The example above can be reproduced
like:

>>> res = api.find_entry(session, id=42, as_result=True)


Reference
~~~~~~~~~

.. autoclass:: metacatalog.util.results.ImmutableResultSet
    :members:

"""
from typing import Union, List
import hashlib
import json
from itertools import chain
from tqdm import tqdm
import pandas as pd

from metacatalog.models import Entry, EntryGroup
from metacatalog.util.exceptions import MetadataMissingError
from metacatalog.util.dict_functions import flatten


class ImmutableResultSet:
    def __init__(self, instance: Union[Entry, EntryGroup]):
        """
        ImmutableResultSet for the given EntryGroup or Entry.
        
        """
        group = None
        members = []

        # check if an Entry was given
        if isinstance(instance, Entry):
            group = ImmutableResultSet.load_base_group(instance)
            if group is None:
                members = ImmutableResultSet.expand_entry(instance)
            else:
                members = [ImmutableResultSet.expand_entry(e) for e in group.entries]
        
        elif isinstance(instance, EntryGroup):
            group = instance
            members = [ImmutableResultSet.expand_entry(e, group) for e in instance.entries]

        # set attributes
        self.group = group
        self._members = ImmutableResultSet.entry_set(members)

        # create a checksum lookup
        self._checksum_lookup = {e.checksum: e for e in self._members}

    @classmethod
    def load_base_group(cls, entry: Entry):
        r"""
        Returns the base EntryGroup of the given Entry.
        The base EntryGroup is the strongest grouping that requires expanding
        of the result set, to load all siblings. 
        If there are no matching groups, None will be returned

        Currently the order of importance is:

        * ``'Composite'``
        * ``'Split dataset'``

        """
        # get the groups and the names
        groups = entry.associated_groups
        type_names = [group.type.name for group in groups]

        # TODO: here we have a hard-coded hierachy
        if 'Composite' in type_names:
            idx = type_names.index('Composite')
            return groups[idx]
        
        # if 'Split dataset' in type_names:
        #     idx = type_names.index('Split dataset')
        #     return groups[idx]
        
        return None

    @classmethod
    def expand_entry(cls, entry: Entry, base_group: EntryGroup = None):
        """
        Expand this Entry to all siblings.

        .. versionchanged:: 0.3.8
            Split datasets are now nested

        """
        # container
        entries = []

        for g in entry.associated_groups:
            # Split datasets are nested
            if g.type.name == 'Split dataset':
                if base_group is not None and base_group.id == g.id:
                    entries.extend([entry])
                else:
                    entries.extend([ImmutableResultSet(g)])
            
            # composites expand completely
            elif g.type.name == 'Composite':
                entries.extend(g.entries)

        return entries

    @classmethod
    def entry_set(cls, entries: List[Entry]):
        """
        Return a set of entries to remove duplicates

        .. versionchanged:: 0.3.8
            Can now handle nested ResultSets

        """
        # flat the list
        if any([isinstance(e, list) for e in entries]):
            entries = list(chain(*entries))
        
        # get the checksums
        checksums = [e.checksum for e in entries]

        # return only the first occurence of each entry
        return [entries[i] for i, _id in enumerate(checksums) if i==checksums.index(_id)]


    def get(self, name: str, default=None):
        """
        Get the given property from the result set.
        This function will iterate all member and nested 
        :class:`Entries <metacatalog.models.Entry>` and return
        the values for the given property from all childs.
        It will return the set of all properties to remove
        duplicated metadata (i.e. for composites and split-
        datasets). If the set has a length of 1, the value
        itself will be returned

        .. versionchanged:: 0.3.8
            get can now handle nested ImmutableResultSets

        Parameters
        ----------
        name : str
            The name (key) of the requested parameter
        
        Returns
        -------
        result : set, any
            The value for the requested property. If more than
            one value is present, the of all values
            will be returned.
 
        """
        occurences = []
        uuids = []

        # create the dictionaries
        for member in [self.group, *self._members]:
            if isinstance(member, ImmutableResultSet):
                val = member.get(name)
            elif not hasattr(member, name):
                continue
            else:
                # get the value or callable
                val = getattr(member, name)
            
            # if val is callable, execute it 
            if callable(val):
                val = val()
            
            # check the datatype - could be a nested list
            if isinstance(val, list):
                exp_val = [v.to_dict() if hasattr(v, 'to_dict') else v for v in val]
            else:
                exp_val = val.to_dict() if hasattr(val, 'to_dict') else val
            
            # append
            occurences.append(exp_val)

            # TODO: this needs to be improved
            if isinstance(member, ImmutableResultSet):
                uuids.append(member.checksum)
            else:
                uuids.append(member.uuid)
        
        # create the set
        occur_md5 = [hashlib.md5(json.dumps(str(o)).encode()).hexdigest() for o in occurences]
        occur_set = [occurences[i] for i, h in enumerate(occur_md5) if occur_md5.index(h) == i]
        
        # needed to trace back the values
        uuid_list = [uuids[i] for i, h in enumerate(occur_md5) if occur_md5.index(h) == i]

        # check return type
        if len(occur_set) == 1:
            return occur_set[0]
        elif len(occur_set) == 0:
            return default
        else:
            if name == 'uuid':
                return occur_set  # we don't need to index uuids by uuids
            
            # index the occurences by the parent uuid
            return {uuid: occur for uuid, occur in zip(uuid_list, occur_set)}

    @property
    def empty(self):
        """
        .. versionadded:: 0.4.1

        Returns True if there is no :class:`Entry <metacatalog.models.Entry>`
        associated to this result.
        """
        return len(self._members) == 0 and self.group is None

    @property
    def uuids(self):
        """
        Return all uuids that form this result set
        """
        # get the group uuid
        uuids = [self.group.uuid] if self.group is not None else []

        # expand member uuids
        uuids.extend([e.uuid for e in self._members if hasattr(e, 'uuid')])

        return uuids

    @property
    def checksums(self):
        """
        .. versionadded:: 0.3.8
        
        Return all checksums of all members
        """
        # get the group checksum
        checksums = [self.group.checksum] if self.group is not None else []

        # expand to all members
        checksums.extend([e.checksum for e in self._members])

        # checksums need to be sorted
        checksums = sorted(checksums)

        return checksums

    @property
    def checksum(self):
        """
        Return the md5 checksum for this result set to easily
        tell it appart from others.
        The checksum is the md5 hash of all contained member checksums.

        .. versionchanged:: 0.3.8
            now hasing md5 of members instead of uuids

        """
        return hashlib.md5(''.join(self.checksums).encode()).hexdigest()

    def contains_uuid(self, uuid: str):
        """
        Check if the given Entry or EntryGroup is 
        present in this result set
        """
        return uuid in self.uuids

    def to_short_info(self):
        """
        Get a short unified version of the results.

        .. todo:: 
            the list of used keys is hardcoded and should
            depend on the variable / group type or be completely
            dynamic

        """
        # hardcode the keys
        keys = ['id', 'uuid', 'title', 'variable', 'datasource']
        
        # build the output dictionary
        return {key: self.get(key) for key in keys}

    def to_dict(self):
        """
        Generate a full dictionary output of this result set.

        """
        # first get a list of all available keys
        keys = []
        for member in [self.group, *self._members]:
            if not hasattr(member, 'to_dict'):
                continue
            keys.extend(member.to_dict().keys())
        
        # get unique keys
        keys = set(keys)

        # build the output dictionary
        return {key: self.get(key) for key in keys}

    def get_data(self, verbose=False, merge=False, **kwargs) -> dict:
        """
        Return the data as a checksum indexed dict
        """
        # output container
        data = dict()

        # create the generator 
        if verbose:
            gen = tqdm((m for m in self._members), total=len(self._members))
        else:
            gen = (m for m in self._members)
        
        # get the data
        for member in gen:
            # load and add
            if isinstance(member, Entry):
                if member.datasource is None:
                    continue
                ds = member.get_data(**kwargs)
                data[member.checksum] = ds
            
            # nested groups
            elif isinstance(member, ImmutableResultSet):
                unmerged = []
                df = pd.DataFrame()

                # load all data from nested groups
                for m in member._members:
                    try:
                        _df = m.get_data(**kwargs)
                    except MetadataMissingError:
                        # that's fine, just move on
                        continue

                    if isinstance(_df, pd.DataFrame):
                        df = pd.concat((df, _df))
                    else:
                        unmerged.append(_df)
                
                # append
                if len(df) > 0 and len(unmerged) > 0:
                    data[member.checksum] = dict(
                        merged=df,
                        unmerged=unmerged
                    )
                elif not df.empty:
                    data[member.checksum] = df
                elif len(unmerged) > 0:
                    data[member.checksum] = unmerged

        # Composites and Split datasets always try to merge
        if self.group is not None and self.group.type.name in ('Composite', 'Split dataset'):
            merge = True

        # handle merging
        out = dict()
        if merge:
            all_data = pd.DataFrame()
            unmerged = {}
            
            # Composite: merge everything that is a DataFrame
            if self.group is not None and self.group.type.name == 'Composite':
                for checksum, dat in data.items():
                    if isinstance(dat, pd.DataFrame):
                        all_data = pd.merge(all_data, dat, left_index=True, right_index=True, how='outer')
                    else:
                        unmerged.update({checksum: dat})
            # Split dataset: concat everything that is a DataFrame
            elif self.group is not None and self.group.type.name == 'Split dataset':
                for checksum, dat in data.items():
                    if isinstance(dat, pd.DataFrame):
                        all_data = pd.concat([all_data, dat])
                    else:
                        unmerged.update({checksum: dat})
            # other EntryGroup types?
            else:
                for checksum, dat in data.items():
                    if isinstance(dat, pd.DataFrame):
                        all_data = pd.merge(all_data, dat, left_index=True, right_index=True, how='outer')
                    else:
                        unmerged.update({checksum: dat})
            
            # specify return type
            if len(unmerged.keys()) == 0 and not all_data.empty:
                out = all_data
            elif len(unmerged.keys()) > 0 and not all_data.emtpy:
                out = dict(unmerged=unmerged, merged=all_data)
            elif len(unmerged.keys()) > 0 and all_data.empty:
                out = unmerged
            else:
                out = dict()
        else:
            out = data
        
        # return
        return out


class ResultList:
    """
    Container class to handle multiple instances of 
    :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet>`.

    """
    def __init__(self, *members: List[Union[ImmutableResultSet, Entry, EntryGroup]]):
        """
        """
        # set up the internal list
        self._internal_list = []
        
        # check types
        if not all([isinstance(m, (ImmutableResultSet, Entry, EntryGroup)) for m in members]):
            raise AttributeError('Unallowed type found in members')

        # lazy load anything that is not already a ImmutableResultSet
        results = [ImmutableResultSet(m) if not isinstance(m, ImmutableResultSet) else m for m in members]

        # set the filtered list
        self._internal_list = self._filter_set(results)

    @property
    def checksums(self):
        return [m.checksum for m in self._internal_list]
    
    def _filter_set(self, members: List[ImmutableResultSet]) -> List[ImmutableResultSet]:
        """
        Uses the checksum of each member to create a set od ImmutableResultSet
        """
        # extract the checksums
        md5s = [m.checksum for m in members]

        # find the first occurence of each
        filtered = [members[i] for i, md5 in enumerate(md5s) if md5s.index(md5) == i]

        return filtered

    def append(self, item: Union[Entry, EntryGroup, ImmutableResultSet]):
        """
        Append a new ImmutableResultSet to the ResultList. Note that the
        list does not allow duplicates and will raise a Warning if the 
        item is already contained.
        You can also pass :class:`Entry <metacatalog.models.Entry>` and
        :class:`EntryGroup <metacatalog.models.EntryGroup>` instances,
        which will be converted to ImmutableResultSets.

        """
        # convert if needed
        if isinstance(item, (Entry, EntryGroup)):
            item = ImmutableResultSet(item)

        # check if exists
        if item.checksum in self:
            print('This ResultList already includes the item')
        else:
            self._internal_list.append(item)

    def index(self, item: Union[ImmutableResultSet, Entry, EntryGroup, str]) -> int:
        """
        Get the index of a member in this list.
        The function accepts many different types, which will be converted to 
        a ImmutableResultSet. If a string is passed, it will first be interpreted
        as a MD5 checksum of the ImmutableResultSet. If no occurence of that string
        is found, *every* member is **recursively** searched for a key of that
        string in the metadata. The first occurence is passed.

        .. note::
            This method is quite slow, due to the type casting, lazy-loading from
            database and the recursive behavior. If you want to search by MD5 hash
            you can directly use the ResultSet.checksums property, which preserves
            the member order.
        
        Raises
        ------
        ValueError : if item is not present
        AttributeError : if unsupported item is passed

        """
        # cast the item if needed
        if isinstance(item, (Entry, EntryGroup)):
            item = ImmutableResultSet(item)
        
        # handle ResultSet
        if isinstance(item, ImmutableResultSet):
            # check if we have this item
            if item in self:
                return self.checksums.index(item.checksum)

        # handle strings
        elif isinstance(item, str):
            # check if the item is a checksum
            if item in self.checksums:
                return self.checksums.index(item)
            
            # otherwise check every member recursively
            for i, member in enumerate(self._internal_list):
                flatdict = flatten(member.to_dict())
                
                # this is a hit
                if item in list(flatdict.values()):
                    return i

        # else the item is unsupported
        else:
            raise AttributeError('The item type is not supported')
        
        # if still not returned, raise the Value Error
        raise ValueError('The item is not in the current ResultList')

    @property
    def temporal_scale(self):
        """
        Common temporal scale triplet for the whole ResultList.
        Only information of :class:`Entries <metacatalog.models.Entry>`
        that hold time scale information are included.
        The common scale triplet is calculated:
        The extent is the maximum period, where **all** members in 
        the list have data. If extent is ``None``, no overlap was
        found. 
        The resolution is the maximum (most coarse) resolution
        in the List, which needs to be aggregated to, if the data
        should be harmonized.
        The support is recalulated by converting the minimum
        support and re-scaling it to the new resolution.  

        Returns
        -------
        triplet : dict
            Dict of extent, resolution and support key.

        See Also
        --------
        TemporalScale : metacatalog.models.TemporalScale

        """
        # first collect the scaling information
        scales = dict(start=[], end=[], resolution=[], support=[])
        
        # read out each resultset
        for result in self._internal_list:
            ds = result.get('datasource')

            # check if exists
            if ds is None:
                continue
            
            sca = []
            # flat reuslts
            if 'temporal_scale' in ds.keys():
                sca.append(ds['temporal_scale'])
            # more than one datasource in result set
            else:
                for s in list(ds.values()):
                    if isinstance(s, dict) and 'temporal_scale' in s:
                        sca.append(s['temporal_scale'])
            
            # go for each scale triplet found
            for s in sca:
                scales['start'].append(s['extent'][0])
                scales['end'].append(s['extent'][1])
                scales['resolution'].append(pd.to_timedelta(s['resolution']).to_pytimedelta())
                scales['support'].append(float(s['support']))
        
        # finally find the smallest
        start = min(scales['start'])
        end = max(scales['end'])
        resolution = max(scales['resolution'])
        # get the smallest support in seconds
        support = min([r.seconds * s for r, s in zip(scales['resolution'], scales['support'])])
        support = support / resolution.seconds

        return dict(
            extent=[start, end] if start < end else [],
            resolution=pd.to_timedelta(resolution).isoformat(),
            support=support
        )

    def __call__(self) -> List[ImmutableResultSet]:
        return self._internal_list

    def __iter__(self):
        yield from self._internal_list

    def __len__(self):
        return len(self._internal_list)

    def __contains__(self, item: ImmutableResultSet) -> bool:
        return item.checksum in [m.checksum for m in self._internal_list]

    def __getitem__(self, key: Union[int, slice]):
        return self._internal_list[key]
    
    def __setitem__(self, key, value):
        raise NotImplementedError("You can't directly set items. Use the append method")
    
    def __str__(self) -> str:
        return self._internal_list.__str__()
    
    def __repr__(self) -> str:
        return self._internal_list.__repr__()
