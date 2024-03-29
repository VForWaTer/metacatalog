"""
The Entry is the core class of metacatalog. It represents the core logical unit of the meta data model.
In principle, an Entry needs a first Author, a title, position and a license to describe
one type of environmental variable. It can hold a reference and interface to the actual data.
If a supported data format is used, Entry can load the data.

"""
from __future__ import annotations
from typing import List, Dict, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from metacatalog.models import License, PersonAssociation, Person, Variable, EntryGroup, Keyword, Detail, DataSource, PersonRole
    from shapely.geometry import Point
import os
from datetime import datetime as dt
import hashlib
import json
from dateutil.relativedelta import relativedelta as rd
from uuid import uuid4
import warnings
from collections import defaultdict

from sqlalchemy import Column, ForeignKey, event
from sqlalchemy import Integer, String, Boolean, DateTime
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape
from sqlalchemy.orm import relationship, backref, object_session, Session

import nltk
import pandas as pd
import numpy as np

from metacatalog.db.base import Base
from metacatalog import models
from metacatalog import api
from metacatalog.util.exceptions import MetadataMissingError, IOOperationNotFoundError
from metacatalog.util.location import around
from metacatalog.util.dict_functions import serialize


def get_embargo_end(datetime=None):
    if datetime is None:
        datetime = dt.utcnow()
    return datetime + rd(years=2)


class Entry(Base):
    r"""
    The Entry is the main entity in metacatalog. An object instance models a
    set of metadata needed to store and manage a datasource. The Entry is not
    the actual data.
    The Entry is designed to store all necessary information to be exportable
    in ISO19115 in the scope of metacatalog. That means, Properties which are
    always the same across metacatalog, or can be derived from the actual
    implementation, are not part of an Entry.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    uuid : str
        .. versionadded:: 0.1.9

        Version 4 UUID string to identify the Entry across installations.
        This field is read-only and will be assigned on creation. It is primarily
        used to export Entry into ISO19115 metadata.
    title : str
        A full title (512) to describe the datasource as well as possible.
        The truncated title (first 25 signs) is usually used to print an
        Entry object to the console.
    abstract : str
        Full abstract of the datasource. The abstract should include all
        necessary information that is needed to fully understand the data.
    external_id : str
        Any kind of OID that was used to identify the data in the first place.
        Usually an unque ID field of other data-storage solutions. The
        exernal_id is only stored for reference reasons.
    location : str, tuple
        .. versionchanged:: 0.6.1

        A POINT location should be specified here if there is a physical measurement 
        point that is different from the centroid of the spatial extent (e.g., 
        discharge measurement with the extent of the catchment).Otherwise, 
        :class:`Datasource.spatial_scale.extent <metacatalog.models.SpatialScale.extent>` 
        should be used to specify the location of the measured data.

        The location as a POINT Geometry in unprojected WGS84 (EPSG: 4326).
        The location is primarily used to show all Entry objects on a map, or
        perform geo-searches.
        The location can be passed as WKT or a tuple of (x, y) coordinates.
        Note that it will be returned and stored as WKB. The output value will
        be reworked in a future release
    creation : datetime.datetime
        Following the ISO19115 the *creation* date is referring to the creation
        date of the **data resource** described by the Entry, not the Entry
        itself. If creation date is not set, it is assumed, that yet no data
        resource is connected to the Entry.
    end : datetime.datimetime
        The last date the data source described by this Entry has data for.
        The end date is **not** ISO19115-compliant and will be reworked.
    version : int
        The version of this Entry. Usually metacatalog will handle the version
        itself and there is not need to set the version manually.
    latest_version_id : int
        Foreign key to ``Entry.id``. This key is self-referencing the another
        Entry. This has to be set if the current Entry is not the latest one.
        If latest_version_id is None, the Entry is the most recent one and
        database operations that find multiple entries will in a future release
        filter to 'version duplicates'.
    is_partial : bool
        .. versionadded:: 0.1.10

        If an Entry is partial, it is not self-contained and **has** to be part
        of a :class:`EntryGroup <metacatalog.models.EntryGroup>` of type
        composite.

        .. note::

            To make it possbile to add partial Entrys via the models submodule,
            The Entry class itself will  **not** check integrity. This has to
            be done on adding partial Entry records, or by checking the database
    comment : str
        Arbitrary free-text comment to the Entry
    citation : str
        .. versionadded:: 0.1.13

        Citation informatio for this Entry. Note, that metacatalog does not
        assign DOIs and thus a citation is only useful if the associated
        data has a DOI and the bibliographic information applies to the Entry
        as well.
        .. note::
            Metacatalog does not manage bibliography. Thus it is highly
            recommended to use thrid party software for management and only
            export the reference to the resource in a common citation style.
    license : metacatalog.models.License
        Data License associated to the data and the metadata. You can pass
        the :class:`License <metacatalog.models.License>` itself, or use the
        license_id attribute.
    license_id : int
        Foreign key to the data license.
    author : metacatalog.models.Person
        :class:`Person <metacatalog.models.Person>` that acts as first author
        for the given entry. Only one first author is possible, co-authors can
        be requested from either the contributors list or the
        :py:attr:`authors` property. ``author`` is a property and setting a
        new author using this property is not supported.
    authors : list
        List of :class:`Person <metacatalog.models.Person>`. The first element
        is the first author, see :py:attr:`~author`. The others are
        :class:`Person <metacatalog.models.Person>`s associated with the
        :class:`Role <metacatalog.models.PersonRole>` of ``'coAuthor' ``.
        The list of authors is sorted by the ``order`` attribute.
        ``authors`` is a property and setting a new list of authors using this
        property is not supported.

    Note
    ----
    One Entry object instance is always described by exactly one variable.
    If a datasource is a composite of many datasources, there are two
    strategies. Either a new table can be implemented and an abstract
    :class:`Variable <metacatalog.models.Variable>` be added. This is done with
    Eddy-Covariance data. Secondly, Each variable of the datasource can be
    represented by its own Entry, which get then grouped by an
    :class:`EntryGroup` of :class:`EntryGroupType` ``'composite'``.

    See Also
    --------
    `EntryGroup`
    `EntryGroupType

    """
    __tablename__ = 'entries'

    # columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(36), nullable=False, default=lambda: str(uuid4()))
    title = Column(String(512), nullable=False)
    abstract = Column(String)
    external_id = Column(String)
    location = Column(Geometry(geometry_type='POINT', srid=4326), nullable=True)
    version = Column(Integer, default=1, nullable=False)
    latest_version_id = Column(Integer, ForeignKey('entries.id'), nullable=True)
    is_partial = Column(Boolean, default=False, nullable=False)
    comment = Column(String, nullable=True)
    citation = Column(String(2048), nullable=True)

    license_id = Column(Integer, ForeignKey('licenses.id'))
    variable_id = Column(Integer, ForeignKey('variables.id'), nullable=False)
    datasource_id = Column(Integer, ForeignKey('datasources.id'))

    embargo = Column(Boolean, default=False, nullable=False)
    embargo_end = Column(DateTime, default=get_embargo_end)

    publication = Column(DateTime, default=dt.utcnow)
    lastUpdate = Column(DateTime, default=dt.utcnow, onupdate=dt.utcnow)

    # relationships
    contributors: List['PersonAssociation'] = relationship("PersonAssociation", back_populates='entry', cascade='all, delete, delete-orphan')
    keywords: List['Keyword'] = relationship("Keyword", back_populates='tagged_entries', secondary="nm_keywords_entries")
    license: 'License' = relationship("License", back_populates='entries')
    variable: 'Variable' = relationship("Variable", back_populates='entries')
    datasource: 'DataSource' = relationship("DataSource", back_populates='entries', cascade='all, delete, delete-orphan', single_parent=True)
    other_versions = relationship("Entry", backref=backref('latest_version', remote_side=[id]))
    associated_groups: List['EntryGroup'] = relationship("EntryGroup", secondary="nm_entrygroups", back_populates='entries')
    details: List['Detail'] = relationship("Detail", back_populates='entry')

    # extensions
    io_extension = None
    io_interface = None

    def to_dict(self, deep: bool = False, stringify: bool = False) -> dict:
        """
        Return the model as a python dictionary.

        .. versionchanged:: 0.7.4

            The dictionary now contains all persons roles

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
        out = dict(
            id=self.id,
            uuid=self.uuid,
            title=self.title,
            author=self.author.to_dict(deep=False),
            authors=[a.to_dict(deep=False) for a in self.authors],
            variable=self.variable.to_dict(deep=False),
            embargo=self.embargo,
            embargo_end=self.embargo_end,
            version=self.version,
            isPartial=self.is_partial,
            publication=self.publication,
            lastUpdate=self.lastUpdate,
            keywords=[kw.to_dict() for kw in self.keywords]
        )

        # optional relations
        if self.location:
            out['locationShape'] = self.location_shape.wkt
            out['location'] = self.location_shape.wkt

        if self.license is not None:
            out['license'] = self.license.to_dict(deep=False)

        if self.details is not None:
            out['details'] = self.details_dict(full=True)

        if self.datasource is not None:
            out['datasource'] = self.datasource.to_dict(deep=False)

        # set optional attributes
        for attr in ('abstract', 'external_id','comment', 'citation'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                out[attr] = getattr(self, attr)

        # add contributors, that are not author or coAuthor
        updates = defaultdict(lambda: [])
        for pa in self.contributors:
            role: str = pa.role.name
            if role.lower() not in ['author', 'conauthor']:
                updates[role].append(pa.person.to_dict(deep=False))

        # update the return dict if there were any updates
        if len(updates) > 0:
            out.update(updates)

        # lazy loading
        if deep:
            projects = self.projects
            if len(projects) > 0:
                out['projects'] = [p.to_dict(deep=False) for p in projects]
            comp = self.composite_entries
            if len(comp) > 0:
                out['composite_entries'] = [e.to_dict(deep=False) for e in comp]

        if stringify:
            return serialize(out, stringify=True)
        return out

    @classmethod
    def from_dict(cls, session: Session, data: dict) -> 'Entry':
        """
        Create a *new* Entry in the database from the given dict.

        .. versionadded:: 0.4.8

        .. versionchanged:: 0.7.4

            PersonRoles other than 'author' and 'coAuthor' can be
            imported as well.

        Parameters
        ----------
        data : dict
            The dictionary to create the Entry from.

        Returns
        -------
        entry : Entry
            The newly created Entry.

        Notes
        -----
        Currently, uploading data sources and data records is not supported.

        """
        if not os.getenv('METACATALOG_SUPRESS_WARN', False):
            warnings.warn("With a future release, the Entry.from_dict method will not create Entries in the database automatically, but instatiate a model. To supress this warning set the METACATALOG_SUPRESS_WARN environment variable.", FutureWarning)
        if 'id' in data:
            raise NotImplementedError('Updating an Entry is not yet supported.')
        
        # create or load variable
        variable_data = data.get('variable', {})
        if 'id' in variable_data:
            variable_id = variable_data['id']
        elif len(variable_data) > 0:
            variable_id = models.Variable.from_dict(variable_data, session).id
        else:
            raise ValueError('No variable data given.')
        
        # create or load author
        author_data = data.get('author', {})
        if 'id' in author_data:
            author_id = author_data['id']
        elif len(author_data) > 0:
            author_id = models.Person.from_dict(author_data, session).id
        else:
            raise ValueError('No author data given.')

        # create or load license
        license_data = data.get('license', {})
        if 'id' in license_data:
            license_id = license_data['id']
        else:
            license_id = None
        
        # add the entry
        entry = api.add_entry(
            session=session,
            title=data['title'],
            author=author_id,
            location=data.get('location'),
            variable=variable_id,
            abstract=data.get('abstract'),
            external_id=data.get('external_id'),
            license=license_id,
            embargo=data.get('embargo', False)
        )

        # add coauthors
        coauthors = [models.Person.from_dict(a, session) for a in data.get('coauthors', [])]
        api.add_persons_to_entries(
            session,
            entries=[entry],
            persons=coauthors,
            roles=['coAuthor'] * len(coauthors),
            order=[_ + 2 for _ in range(len(coauthors))]
        )

        # load all available
        available_roles: List['PersonRole'] = api.find_role(session)
        for role in available_roles:
            if not hasattr(data, role.name) or role.name in ['author', 'coAuthor']:
                continue
            else:
                contributors = [models.Person.from_dict(a, session) for a in data[role.name]]
            
            # otherwise add
            api.add_persons_to_entries(
                session,
                entries=[entry],
                persons=[contributors],
                roles=[role.name] * len(contributors)
            )

        # add keywords
        keyword_uuids = data.get('keywords', [])
        if len(keyword_uuids) > 0:
            keywords = [api.get_uuid(session, uuid) for uuid in keyword_uuids]
            api.add_keywords_to_entries(session=session, entries=[entry], keywords=keywords)

        # add details
        details = data.get('details', [])
        if len(details) > 0:
            api.add_details_to_entries(session, [entry], data.get('details', []))

        return entry

    @classmethod
    def is_valid(cls, entry: 'Entry') -> bool:
        return isinstance(entry, Entry) and entry.id is not None

    @property
    def checksum(self) -> str:
        """
        MD5 checksum of this entry. The checksum will change if any of the linked
        Metadata changes. This can be used in application built on metacatalog to
        verify integrity.

        .. versionadded:: 0.3.8

        """
        # get a dict_representation
        d = self.to_dict(deep=True, stringify=True)
        
        # calculate the hash
        md5 = hashlib.md5(json.dumps(d).encode()).hexdigest()

        return md5

    @property
    def is_latest_version(self) -> bool:
        self.latest_version_id == self.id or self.latest_version_id is None

    @property
    def latest_version(self) -> 'Entry':
        versions = [e.version for e in self.other_versions]

        # no other versions, then self is the only
        if len(versions) == 1:
            return self

        # if more versions exist, find the highest number
        latest_index = versions.index(max(versions))
        return self.other_versions[latest_index]

    @property
    def author(self) -> 'Person':
        return [c.person for c in self.contributors if c.role.name == 'author'][0]

    @author.setter
    def author(self, new_author: 'Person'):
        self.set_new_author(new_author)

    def set_new_author(self, new_author: 'Person', commit: bool = False):
        """
        Set a new first Author for this entry.

        Parameters
        ----------
        new_author : metacatalog.models.Person
            The new first author. As of now the new author has to be passed as a
            model instance. Passing the ID or query parameter is not yet supported.
        commit : boolean
            If True, the whole :class:`Entry <metacatalog.models.Entry>` will commit
            and persist itself to the database.
            
            .. note::

                This will also affect other uncommited edits to the Entry.

        """
        if not isinstance(new_author, models.Person):
            raise AttributeError('The new author has to be of type metatacatalog.models.Person')

        # find the association
        assoc_idx = [i for i, c in enumerate(self.contributors) if c.role.name == 'author'][0]
        self.contributors[assoc_idx].person = new_author

        if commit:
            session = object_session(self)
            try:
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    @property
    def authors(self) -> List['Person']:
        # get all
        coAuthors = [c for c in self.contributors if c.role.name == 'coAuthor']

        # order
        idx = np.argsort([c.order for c in coAuthors])

        # build the author list
        authors = [self.author]
        for i in idx:
            authors.append(coAuthors[i].person)

        return authors

    @property
    def projects(self) -> List['EntryGroup']:
        return [group for group in self.associated_groups if group.type.name.lower() == 'project']

    @property
    def composite_entries(self):
        return [group for group in self.associated_groups if group.type.name.lower() == 'composite']

    @property
    def location_shape(self) -> 'Point' | None:
        if self.location:
            return to_shape(self.location)
        else:
            return None

    @location_shape.setter
    def location_shape(self, shape):
        self.location = from_shape(shape)

    def keyword_list(self) -> List[str]:
        """
        List of tagged keywords associated to this instance. 
        The keywords are related via the association table.

        """
        # keywords
        return [kw.path() for kw in self.keywords]

    def plain_keywords_list(self) -> List[str]:
        """
        Returns list of controlled keywords associated with this
        instance of meta data.
        The List only includes the full path

        """
        warnings.warn("Entry.plain_keyword_list is deprecated since 0.7.3, use Entry.keyword_list instead.", category=DeprecationWarning)

        return self.keyword_list()

    def plain_keywords_dict(self) -> List[Dict[str, str]]:
        """
        Get a list of dictionaries containing a dict representation
        of each associated keyword to this Entry.

        """
        warnings.warn("plain_keywords_dict is deprecated. Use [k.to_dict() for k in Entry.keywords] instead.", category=DeprecationWarning)
        return [kw.to_dict() for kw in self.keywords]

    def keywords_dict(self):
        """
        """
        warnings.warn("keywords_dict is deprecated and will be removed with a future release", category=DeprecationWarning)
        return [
            dict(
                path=kw.full_path,
                value=kw.value
            ) for kw in self.keywords
        ]

    def details_dict(self, full: bool = True) -> dict:
        """
        Returns the associated details as dictionary.

        Parameters
        ----------
        full : bool
            If True (default) the keywords will contain the
            full info including key description, ids and
            stemmed key. If false, it will be truncated to a
            plain key:value dict

        """
        if full:
            return {d.stem:d.to_dict() for d in self.details}
        else:
            return {d.stem:d.value for d in self.details}

    def details_table(self, fmt: str = 'html') -> str:
        """
        Return the associated details as table

        Parameters
        ----------
        fmt : string
            Can be one of:

            * ``html`` to return a HTML table
            * ``latex`` to return LaTeX table
            * ``markdown`` to return Markdown table

        """
        # get the details
        details = dict()
        for key, detail in self.details_dict(full=True).items():
            # nested details
            if isinstance(detail['value'], dict):
                # include top-level detail of nested detail
                details[key] = detail.copy()
                details[key]['value'] = '-'
                # go for nested details
                for k, v in detail['value'].items():
                    expand = {
                        f'{key}.{k}': dict(
                            value=v,
                            id=detail['id'],
                            key=f"{key}.{k}",
                            stem=detail['stem'],
                            entry_id=detail['entry_id'],
                            entry_uuid=detail['entry_uuid']
                        )
                    }
                    details.update(expand)
            else:
                details[key] = detail

        # turn into a transposed datarame
        df = pd.DataFrame(details).T

        # output table
        if fmt.lower() == 'html':
            return df.to_html()
        elif fmt.lower() == 'latex':
            return df.to_latex()
        elif fmt.lower() == 'markdown' or fmt.lower() == 'md':
            return df.to_markdown()
        else:
            raise ValueError("fmt has to be in ['html', 'latex', 'markdown']")

    def add_details(self, details=None, commit: bool = False, **kwargs) -> None:
        """
        Adds arbitrary key-value pairs to this entry.

        Parameters
        ----------
        details : list
            .. versionadded:: 0.1.8

            List of dict of structure:
            
            .. code-block:: text

                [{
                    'key': '',
                    'value': '',
                    'description': ''
                }]
        
            where the ``description`` is optional and can be omitted.
            If no descriptions are passed at all, you can also use `**kwargs`
            to pass ``key=value`` pairs.
        commit : bool
            If True, the Entry session will be added to the
            current session and the transaction is commited.
            Can have side-effects. Defaults to False.

        """
        ps = nltk.PorterStemmer()

        # build entries here
        detail_list = []

        # parse kwargs
        for k, v in kwargs.items():
            detail_list.append({
                'entry_id': self.id, 
                'key': str(k), 
                'stem': ps.stem(k), 
                'value': v
            })

        # parse details
        if details is not None:
            for detail in details:
                d = {
                    'entry_id': self.id,
                    'key': detail['key'],
                    'stem': ps.stem(detail['key']),
                    'value': detail['value']
                }
                if 'description' in detail.keys():
                    d['description'] = detail['description']
                detail_list.append(d)

        # build the models
        for detail in detail_list:
            self.details.append(models.Detail(**detail))

        if commit:
            session = object_session(self)
            try:
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    def export(self, path: str = None, fmt: str = 'JSON', **kwargs):
        r"""
        Export the Entry. Exports the data using a metacatalog extension.
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
                def json(cls, entry, path, **kwargs):
                    # get the dict
                    data = entry.to_dict(stringify=True)
                    if path is None:
                        return data
                    else:
                        with open(path, 'w') as f:
                            json.dump(data, f, indent=kwargs.get('indent', 4))
        
        You can activate and use it like:

        >> from metacatalog import config
        >> config.load_extension('export', RawJSONEXtension)
        >> entry.export(path='testfile.json', fmt='json', indent=2)

        """
        # load the config module
        from metacatalog import config
        # load the extension
        try:
            Export = config.extension(f'export-{fmt.lower()}').interface
        except AttributeError:
            try:
                Export = config.extension('export').interface
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

    def make_composite(self, others: List['Entry'] = [], title: str = None, description: str = None, commit: bool = False) -> 'EntryGroup':
        """
        Create a composite EntryGroup from this Entry. A composite marks
        stand-alone (:attr:`is_partial` ``= False``) entries as inseparable.
        A composite can also contain a partial Entry
        (:attr:`is_partial` ``= True``), whichs  data only makes sense in the
        context of the composite group.

        Parameters
        ----------
        others : list of Entry
            The other :class:`Entries <metacatalog.models.Entry>` that
            should be part of the composite.
        title : str
            Optional title of the composite, if applicable
        description : str
            Optional description of the composite if applicable
        commit : bool
            If True, the newly created Group will be persisted in the
            database. Defaults to False.

        Returns
        -------
        composite : metacatalog.models.EntryGroup
            The newly created EntryGroup of EntryGroupType.name == 'Composite'

        """
        # check type of others
        if isinstance(others, Entry):
            others = [others]
        if not all([isinstance(e, Entry) for e in others]):
            raise AttributeError("others has to be a list of Entry instances")
        others.append(self)

        # get a session
        session = object_session(self)
        type_ = api.find_group_type(session, name='Composite')[0]
        composite = models.EntryGroup(type=type_, title=title, description=description, entries=others)

        if commit:
            try:
                session.add(composite)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

        # return
        return composite

    def neighbors(self, distance: Union[int, float], unit: str = 'meter', buffer_epsg: int = 3857, as_sql: bool = False, **kwargs) -> List['Entry']:
        """
        Find neighboring :class:`Entries <metacatalog.models.Entry>` around the
        location of this instance. You can return the result, or the sqlalchemy
        Query object, which can be printed as plain SQL.

        Parameters
        ----------
        distance : int, float
            The maximum distance at which another Entry is still considered to be a neighbor.
        unit : str
            Has to be one of ['meter', 'km', 'mile', 'nautic'] to specify the unit
            of the given distance. Note that the distance will always be transformed
            into meter.
        buffer_epsg : int
            The EPSG identification number of any projected cartesian coordinate
            reference system that uses meter as unit. This CRS will be used to
            apply the search distance (in meter).

            .. note::

                The default system is the transversal Mercartor projection, which is
                a global system. Thus, it can always be applied, but may introduce
                large uncertainties in small areas. Replace this attribute by a
                local CRS wherever possible.

        as_sql : bool
            If False (default) the SQL query for neighbors will be executed and
            the result is returned. Else, the SQL query itself will be returned.
        kwargs : keyword arguments
            Any passed keyword argument will be passed down to the
            :func:`api.find_entry <metacatalog.api.find_entry>` function to further
            filter the results.

        See Also
        --------
        :func:`around <metacatalog.util.location.around>`
        :func:`find_entry <metacatalog.api.find_entry>`

        """
        # open a session
        session = object_session(self)

        # get the base filter query
        kwargs['return_iterator'] = True
        query = api.find_entry(session, **kwargs)

        # get the area
        filter_query = around(self, distance=distance, unit=unit, query=query, buffer_use_epsg=buffer_epsg)

        if as_sql:
            return filter_query
        else:
            return filter_query.all()

    def create_datasource(self, path: str, type: Union[int, str], datatype: Union[int, str], variable_names: List[str] = None, commit: bool = False, **args) -> 'DataSource':
        """
        Create a :class:`DataCource <metacatalog.models.DataSource>` for this
        Entry. The data-source holds specific metadata about the actual type 
        of source, the data resides in.

        Parameters
        ----------
        path : str
            The path to the data. This depends on the type of datasource used.
            This can be a URI, database table, local path etc.
        type : metacatalog.models.DataSourceType
            type of the datasource
        datatype : metacatalog.models.DataType
            Data Type of the referenced source
        variable_names : list, optional
            .. versionadded:: 0.9.1

            List of variable names that store the data of the datasource of the entry.
            In tabular data, this is usually the column name(s) of the variable that
            is referenced by the Entry. In case of a netCDF file, this is the variable
            name(s) of the variable(s) that is/are referenced by the Entry.  
            More generally, variable_names describes how a datasource would be indexed
            to retrieve the data of the entry.        
        commit : bool, optional
            If true, the created datasource will directly be commited to the
            database. If false (default) the model will be created and linked
            to this Entry, but you still need to add it to a database session
            and commit the session.

        Returns
        -------
        datasource : metacatalog.models.DataSource
            Returns the newly created Datasource, which is also available as
            ``Entry.datasource``.

        """
        #
        if self.datasource is not None:
            raise MetadataMissingError('Datasource already exists. You can edit that one.')

        # get a session
        session = object_session(self)

        # load the datasource type
        if isinstance(type, int):
            ds_type = api.find_datasource_type(session=session, id=type, return_iterator=True).one()
        elif isinstance(type, str):
            ds_type = api.find_datasource_type(session=session, name=type, return_iterator=True).first()
        else:
            raise AttributeError('type has to be of type int or str')

        # TODO need the API for DataTypes here!!
        dtype = session.query(models.DataType).filter(models.DataType.name==datatype).one()

        # build the datasource object
        ds = models.DataSource(type=ds_type, datatype=dtype, path=path, variable_names=variable_names)

        # add the args
        ds.save_args_from_dict(args)

        # append to self
        self.datasource = ds

        if commit:
            try:
                session.add(self)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

        # return
        return ds

    def get_data(self, **kwargs):
        """
        Read the data. This is only possible if a datasource is specified and
        any kind of IOExtension or IOInterface is activated. By default,
        the builtin :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>`
        is activated since version 0.1.12.

        .. versionchanged:: 0.1.12

        """
        if self.datasource is None:
            raise MetadataMissingError('Entry need datasource information')

        try:
            # check if an io_extension is set
            if self.io_extension is not None:
                return self.io_extension.read(**kwargs)

            # if no extension instance, maybe an interface class is set
            elif self.io_interface is not None:
                reader = self.io_interface.get_reader(self.datasource)
                return reader(self, self.datasource, **kwargs)
            else:
                raise IOOperationNotFoundError("No IO interface activated. Run metacatalog.ext.extension('io', InterfaceClass) to register")
        except IOOperationNotFoundError as e:
            print('[ERROR]: Operation not possible.\n%s' % str(e))
            return None

    def import_data(self, data, **kwargs):
        """
        Import data. This is only possible if a datasource is specified and
        any kind of IOExtension or IOInterface is activated. By default,
        the builtin :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>`
        is activated since version 0.1.12.

        For the default interface, the datasource type and data type determine
        where the data will be stored and how the data has to look like.
        You can easily inherit from the
        :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>` to
        customize read and write behaviour. If you import i.e. a timeseries to
        the same database as metacatalog, you will need to prepared data to
        to only hold an datetime index and the data to be stored.

        .. versionchanged:: 0.1.12

        """
        if self.datasource is None:
            raise MetadataMissingError('Entry need datasource information')

        try:
            # check if an io_extension is set
            if self.io_extension is not None:
                return self.io_extension.import_(data, **kwargs)

            # if no extension instance, maybe an interface class is set
            elif self.io_interface is not None:
                importer = self.io_interface.get_importer(self.datasource)
                return importer(self, self.datasource, data, **kwargs)
            else:
                raise IOOperationNotFoundError("No IO interface activated. Run metacatalog.ext.extension('io', InterfaceClass) to register")
        except IOOperationNotFoundError as e:
            print('[ERROR]: Operation not possible.\n%s' % str(e))
            return None

    def append_data(self, data, **kwargs):
        """
        Append data. This is only possible if a datasource is specified and
        any kind of IOExtension or IOInterface is activated. By default,
        the builtin :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>`
        is activated since version 0.1.12.

        For the default interface, the datasource type and data type determine
        where the data will be stored and how the data has to look like.
        You can easily inherit from the
        :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>` to
        customize read and write behaviour. If you import i.e. a timeseries to
        the same database as metacatalog, you will need to prepared data to
        to only hold an datetime index and the data to be stored.

        .. versionadded:: 0.1.12

        """
        if self.datasource is None:
            raise MetadataMissingError('Entry need datasource information')

        try:
            # check if an io_extension is set
            if self.io_extension is not None:
                return self.io_extension.append(data, **kwargs)

            # if no extension instance, maybe an interface class is set
            elif self.io_interface is not None:
                appender = self.io_interface.get_appender(self.datasource)
                return appender(self, self.datasource, data, **kwargs)
            else:
                raise IOOperationNotFoundError("No IO interface activated. Run metacatalog.ext.extension('io', InterfaceClass) to register")
        except IOOperationNotFoundError as e:
            print('[ERROR]: Operation not possible.\n%s' % str(e))
            return None

    def delete_data(self, delete_source=False, **kwargs):
        """
        Delete data. This is only possible if a datasource is specified and
        any kind of IOExtension or IOInterface is activated. By default,
        the builtin :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>`
        is activated since version 0.1.12.

        For the default interface, the datasource type and data type determine
        where the data is stored and how the data will be delted.
        You can easily inherit from the
        :class:`IOExtension <metacatalog.ext.io.extension.IOExtension>` to
        customize read and write behaviour.

        .. versionadded:: 0.1.12

        Parameters
        ----------
        delete_source : bool
            If True, the DataSource will be deleted as well after the data
            has been deleted.

        """
        if self.datasource is None:
            raise MetadataMissingError('Entry need datasource information')

        kwargs['delete_source'] = delete_source
        try:
            # check if an io_extension is set
            if self.io_extension is not None:
                return self.io_extension.delete(**kwargs)

            # if no extension instance, maybe an interface class is set
            elif self.io_interface is not None:
                deleter = self.io_interface.get_deleter(self.datasource)
                return deleter(self, self.datasource, **kwargs)
            else:
                raise IOOperationNotFoundError("No IO interface activated. Run metacatalog.ext.extension('io', InterfaceClass) to register")
        except IOOperationNotFoundError as e:
            print('[ERROR]: Operation not possible.\n%s' % str(e))
            return None

    def __str__(self) -> str:
        return "<ID=%d %s [%s] >" % (
            self.id,
            self.title[:20],
            self.variable.name
            )
