from typing import List, TYPE_CHECKING
if TYPE_CHECKING:
    from metacatalog.models import Entry
import hashlib
from metacatalog.util.dict_functions import serialize
from uuid import uuid4
from datetime import datetime as dt

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import relationship

from metacatalog.db.base import Base


class EntryGroupAssociation(Base):
    __tablename__ = 'nm_entrygroups'

    entry_id: int = Column(Integer, ForeignKey('entries.id'), primary_key=True)
    group_id: int = Column(Integer, ForeignKey('entrygroups.id'), primary_key=True)


class EntryGroupType(Base):
    __tablename__ = 'entrygroup_types'

    # columns
    id: int = Column(Integer, primary_key=True)
    name: str = Column(String(40), nullable=False)
    description: str = Column(String, nullable=False)

    # relationships
    groups: List['EntryGroup'] = relationship("EntryGroup", back_populates='type')

    def to_dict(self, deep: bool = False) -> dict:
        """To dict

        Return the model as a python dictionary.

        Parameters
        ----------
        deep : bool
            If True, all related objects will be included as
            dictionary. Defaults to False

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            id=self.id,
            name=self.name,
            description=self.description
        )

        # lazy loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    def __str__(self) -> str:
        return "%s <ID=%d>" % (self.name, self.id)


class EntryGroup(Base):
    """
    An EntryGroup is an association object between any number of
    :class:`Entry <metacatalog.models.Entry>` records. The type
    of association is further described by
    :class:`EntryGroupType <EntryGroupType>`.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    uuid : str
        .. versionadded:: 0.1.9

        Version 4 UUID string to identify the Entry across installations.
        This field is read-only and will be assigned on creation. It is primarily
        used to export Entry into ISO19115 metadata.
    type : metacatalog.models.EntryGroupType
        The type is most important to give meaning to a group of
        entries. Three types ship with metacatalog ``'Project'``, ``'Composite'`` and ``'Split dataset'``.
    type_id : int
        Foreign key to the EntryGroupType
    title : str
        A descriptive title for the Group. Maximum of 40 letters.
    description : str
        An optional full text description of the EntryGroup. This
        description should contain all information necessary to
        understand the grouping.
    publication : datetime.datetime
        .. versionadded:: 0.1.9

        Autogenerated field giving the UTC timestamp of record creation.
    lastUpdate : datetime.datetime
        .. versionadded: 0.1.9

        Autogenerated field giving the UTC timestamp of last edit.

    """
    __tablename__ = 'entrygroups'

    # columns
    id: int = Column(Integer, primary_key=True)
    uuid: str = Column(String(36), nullable=False, default=lambda: str(uuid4()))
    type_id: int = Column(Integer, ForeignKey('entrygroup_types.id'), nullable=False)
    title: str = Column(String(250))
    description: str = Column(String)

    publication = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)


    # relationships
    type: 'EntryGroupType' = relationship("EntryGroupType", back_populates='groups')
    entries: List['Entry'] = relationship("Entry", secondary="nm_entrygroups", back_populates="associated_groups")

    @property
    def checksum(self) -> str:
        """
        .. versionadded:: 0.3.9

        MD5 checksum of this entry. The checksum will change if any of the linked
        Entries' checksum changes. This can be used in application built on metacatalog to
        verify integrity.
        """
        # get the md5 hashes of all childs
        md_list = ''.join([e.checksum for e in self.entries])
        
        # create the checksum of checksums
        md5 = hashlib.md5(md_list.encode()).hexdigest()

        return md5

    def to_dict(self, deep: bool = False, stringify: bool = False) -> dict:
        """To dict

        Return the model as a python dictionary.

        .. versionchanged:: 0.3.8
            added stringify option

        Parameters
        ----------
        deep : bool
            If True, all related objects will be included as
            dictionary. Defaults to False
        stringify : bool
            If True, all values will be turned into a string,
            to make the object serializable.

        Returns
        -------
        obj : dict
            The Model as dict

        """
        # base dictionary
        d = dict(
            id=self.id,
            uuid=self.uuid,
            type=self.type.to_dict(deep=False),
            publication=self.publication,
            lastUpdate=self.lastUpdate
        )

        # set optionals
        for attr in ('title', 'description'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # lazy loading
        if deep:
            d['entries'] = [e.to_dict(stringify=stringify) for e in self.entries]
        else:
            d['entries'] = [e.uuid for e in self.entries]

        # serialize
        if stringify:
            d = serialize(d, stringify=True)

        return d

    def export(self, path: str = None, fmt: str = 'JSON', **kwargs):
        r"""
        Export the EntryGroup. Exports the data using a metacatalog extension.
        Refer to the note below to learn more about export extensions.

        Parameters
        ----------
        path : str
            If set, the export will be written into a file at the given
            location.
        fmt : str
            Export format. Each export extension should at least support
            json and XML export.
        **kwargs
            Any other argument given will be passed down to the actual
            export function.

        Notes
        -----
        Uses any extension prefixed with 'export-' activated, by passing
        itself to the extension. If not format-specific extension is activated,
        the default :class:`ExportExtension <metacatalog.ext.export.ExportExtension>`
        will be used. A method of same name as ``fmt`` on the extension will be used. 
        If such a method is not present, the 'export' method is used and the fmt 
        attribute will be passed along. This can be used for format specific
        extensions.
        Refer to the notes about :any:`custom extensions <metacatalog.ext.base>`
        to learn more about writing your own export extension.

        Consider this example:

        .. code-block:: Python

            from metacatalog.ext import MetacatalogExtensionInterface
            import json

            class RawJSONExtension(MetacatalogExtensionInterface):
                @classmethod
                def init_extension(cls):
                    pass
                
                @classmethod
                def json(cls, group, path, **kwargs):
                    # get the dict
                    data = group.to_dict(stringify=True)
                    if path is None:
                        return data
                    else:
                        with open(path, 'w') as f:
                            json.dump(data, f, indent=kwargs.get('indent', 4))
        
        You can activate and use it like:

        >> from metacatalog import ext
        >> ext.extension('export', RawJSONEXtension)
        >> group.export(path='testfile.json', fmt='json', indent=2)

        """
        # load the extension
        from metacatalog import ext
        try:
            Export = ext.extension(f'export-{fmt.lower()}')
        except AttributeError:
            try:
                Export = ext.extension('export')
            except AttributeError:
                from metacatalog.ext.export import ExportExtension as Export
        
        # get the export function
        if  hasattr(Export, fmt.lower()):
            exp_function = getattr(Export, fmt.lower())
        elif hasattr(Export, 'export'):
            exp_function = getattr(Export, 'export')
        else:    
            raise AttributeError(f'The current export extension cannot export {fmt}')

        # return
        return exp_function(self, path=path, **kwargs)


    def __str__(self) -> str:
        return "%s%s <ID=%d>" % (
            self.type.name,
            " %s" % self.title[:20] if self.title is not None else '',
            self.id
        )
