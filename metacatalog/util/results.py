"""
Result Set
----------

The ImmutableResultSet class can be used to wrap a list of
:class:`Entry <metacatalog.models.Entry>` or 
:class:`EntryGroup <metacatalog.models.EntryGroup>` instances
a deliver a unified interface to load metadata from nested 
and / or grouped results.

"""
from typing import Union, List
import hashlib
import json
from itertools import chain

from metacatalog.models import Entry, EntryGroup


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
            members = [ImmutableResultSet.expand_entry(e) for e in instance.entries]

        # set attributes
        self.group = group
        self._members = ImmutableResultSet.entry_set(members)

    @classmethod
    def load_base_group(cls, entry: Entry):
        # get the groups and the names
        groups = entry.associated_groups
        type_names = [group.type.name for group in groups]

        # TODO: here we have a hard-coded hierachy
        if 'Composite' in type_names:
            idx = type_names.index('Composite')
            return groups[idx]
        
        if 'Split dataset' in type_names:
            idx = type_names.index('Split dataset')
            return groups[idx]
        
        return None

    @classmethod
    def expand_entry(cls, entry: Entry):
        """
        Expand this Entry to all siblings
        """
        # container
        entries = []

        for g in entry.associated_groups:
            if g.type.name not in ('Composite', 'Split dataset'):
                continue
            entries.extend(g.entries)

        return entries

    @classmethod
    def entry_set(cls, entries: List[Entry]):
        """
        Return a set of entries to remove duplicates
        """
        # flat the list
        if any([isinstance(e, list) for e in entries]):
            entries = list(chain(*entries))
        
        # get the ids
        ids = [e.id for e in entries]

        # return only the first occurence of each entry
        return [entries[i] for i, _id in enumerate(ids) if i==ids.index(_id)]


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

        # create the dictionaries
        for member in [self.group, *self._members]:
            if not hasattr(member, name):
                continue
            val = getattr(member, name)
            # check the datatype - could be a nested list
            if isinstance(val, list):
                exp_val = [v.to_dict() if hasattr(v, 'to_dict') else v for v in val]
            else:
                exp_val = val.to_dict() if hasattr(val, 'to_dict') else val
            occurences.append(exp_val)
        
        # create the set
        occur_md5 = [hashlib.md5(json.dumps(str(o)).encode()).hexdigest() for o in occurences]
        occur_set = [occurences[i] for i,h in enumerate(occur_md5) if occur_md5.index(h) == i]

        # check return type
        if len(occur_set) == 1:
            return occur_set[0]
        elif len(occur_set) == 0:
            return default
        else:
            return occur_set

    @property
    def uuids(self):
        """
        Return all uuids that form this result set
        """
        # get the group uuid
        uuids = [self.group.uuid] if self.group is not None else []

        # expand member uuids
        uuids.extend([e.uuid for e in self._members])

        return uuids

    @property
    def checksum(self):
        """
        Return the md5 checksum for this result set to easily
        tell it appart from others.
        The checksum is the md5 hash of the uuids.
        """
        return hashlib.md5(''.join(self.uuids).encode()).hexdigest()

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