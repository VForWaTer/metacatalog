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
        
        # Composites always try to merge
        if self.group is not None and self.group.type.name == 'Composite':
            merge = True

        # handle merging
        out = dict()
        if merge:
            all_data = pd.DataFrame()
            unmerged = {}
            
            # merge everything that is a DataFrame
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