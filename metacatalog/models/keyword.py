from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, backref


from metacatalog.db.base import Base


class Thesaurus(Base):
    """
    .. versionadded:: 0.1.10
    A thesaurus is a controlled dictionary, which is served at 
    a permanent URL. As of now, metacatalog implements NASA 
    Global Climate change Master Dictionary (GCMD) science keywords.
    
    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    uuid : str
        Version 4 UUID string to identify the Thesaurus across installations. 
        Usually, this is created outside of metacatalog. 
    name : str
        Short name of the thesaurus entry. Should be ``'custom'`` for 
        non-official thesaurii. Example: ``'GCMD'`` for the NASA/GCMD Science
        Keywords, which can be loaded by default.
    title : str
        Full name of the keyword list. The title should contain the full 
        qualified name. It is best practice to use a title the distiributor  
        officially uses for the list. A short name can be given as :attr:`name` 
        attribute.
    organisation : str
        Name of the publishing Organisation for the thesaurus. It has to be 
        the responsible party for the resource given at `url`.
    description : str
        Full description of the scope and context of the thesaurus. From this
        description it should become obvious how keywords relate to the matadata
        that is tagged by the keywords
    url : str
        URL of the full keyword list. May contain a placehoder for an UUID 
        if the API at the resource is capable of loading keywords by UUID.

    Notes
    -----
    If you add other keywords, a thesaurus isnstance has to be 
    specfied, which can be found at the url. If only one url is 
    given, a full XML list of all keywords have to be published here.
    The url may contain a UUID placeholder for loading keywords 
    by UUID. **Do not reference a WIKI page or description via URL**.

    If the server instance operating metacatalog implements custom 
    keyword lists, you can add a thesaurus instance here on 
    installation. Do not expose this table to the user.

    """
    __tablename__ = 'thesaurus'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(64), unique=True, nullable=False)
    name = Column(String(1024), unique=True, nullable=False)
    title = Column(String, nullable=False)
    organisation = Column(String, nullable=False)
    description = Column(String, nullable=True)
    url = Column(String, nullable=False)

    # relationships
    keywords = relationship("Keyword", back_populates="thesaurusName")

    def to_dict(self, deep=False) -> dict:
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
            uuid=self.uuid,
            name=self.name,
            title=self.title,
            organisation=self.organisation,
            url=self.url
        )

        if self.description is not None:
            d['description'] = self.description

        if deep:
            d['keywords'] = [k.to_dict(deep=False) for k in self.keywords]

        return d

    def __str__(self):
        return '%s Thesaurus <ID=%d>' % (self.name, self.id)


class Keyword(Base):
    """
    the major purpose of a Keyword is to describe a Metadataset by 
    a common word. Ideally, these keywords are hosted in a controlled 
    dictionary. That means a third party distributes and describes 
    the scope of these keywords and makes them publicly available. 
    Added to an :class:`Entry <metacatalog.models.Entry>`, the 
    keyword is primarily used to filter entities by keyword. 
    
    Keywords can be used to filter for similar Metadata in 
    cross-database repositories.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    uuid : str
        Version 4 UUID string to identify the Thesaurus across installations. 
        Usually, this is created outside of metacatalog. 
    parent_id : int
        Foreign key to another Keyword (keywords.id). Used to build the 
        hierachical order of keywords.
    value : str
        The keyword itself. Should be all uppercase letters
    full_path : str
        The full keyword including all ancestors keyword values. Usually, 
        the term 'Keyword' refers to the full path of the keyword.
        .. note::
            The `Keyword` does also have a method `Keyword.path` to 
            *contruct* the path by recursively querying the parents' 
            values. You can use this function to store the result in 
            `full_path` for convenience.
    thesaurusName : metacatalog.models.Thesaurus
        .. versionadded:: 0.1.10 
        If the keyword is part of a controlled dictionary, this 
        *thesaurus* should be described here. Usually, metacatalog will 
        only implement onr or two, which are maintained by the admin.
    thesaurus_id : int
        .. versionadded:: 0.1.10
        Foreign key to the associated thesaurus.
    tagged_entries : list
        List of Associations between the current keyword and 
        :class:`Entries <metacatalog.models.Entry>` tagged by this keyword.
        This list is filled automatically.

        .. see-also::
            metacatalog.api.add_keywords_to_entries

    Note
    ----
    If you use additional keywords, that are already part of a 
    controlled dictionary, make sure not to upload them via the 
    metacatalog API, as this will assign new UUIDs to the keywords.
    Rather use this model to upload them directly using sqlalchemy.

    Keywords are build hierachical, separated by ``' > '``. 
    This makes it possible to filter by keywords themselves or 
    by groups of keywords. They have to follow the pattern:

    .. code-block::
        category > topic > term ...
    
    followed by any number of more granular groupings.

    """
    __tablename__ = 'keywords'

    # columns
    id = Column(Integer, primary_key=True)
    uuid = Column(String(64), unique=True)
    parent_id = Column(Integer, ForeignKey('keywords.id'))
    value = Column(String(1024), nullable=False)
    full_path = Column(String)

    # thesaurus column
    thesaurus_id = Column(Integer, ForeignKey('thesaurus.id'))

    # relationships
    children = relationship("Keyword", backref=backref('parent', remote_side=[id]))
    tagged_entries = relationship("KeywordAssociation", back_populates='keyword')
    thesaurusName = relationship("Thesaurus", back_populates="keywords")

    def path(self):
        """Keyword path

        Returns the full keyword path for the given level. 
        The levels are separated by a '>' sign. The levels are:

        Topic > Term > Variable_Level_1 > Variable_Level_2 > Variable_Level_3 > Detailed_Variable

        Returns
        -------

        path : str
            The full keyword path
        """
        path = [self.value]
        parent = self.parent

        while parent is not None:
            path.append(parent.value)
            parent = parent.parent
        
        return ' > '.join(reversed(path))

    def as_dict(self):
        """
        .. deprecated:: 0.1.10
            Use `Keyword.to_dict`. Will be removed in version 0.2
        """
        return {
            'path': self.path(),
            'uuid': self.uuid
        }

    def to_dict(self, deep=False) -> dict:
        """To dict
        .. versionadded:: 0.1.10

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
            uuid=self.uuid,
            value=self.value,
            path=self.path(),
            children=[k.uuid for k in self.children]
        )

        if deep:
            d['thesaurusName'] = self.thesaurusName.to_dict(deep=False)
            d['children'] = [k.to_dict(deep=True) for k in self.children]
            d['tagged_entries'] = [e.entry.uuid for e in self.tagged_entries]
        else:
            d['thesaurus_id'] = self.thesaurus_id
        
        return d

    def __str__(self):
        return "%s <ID=%d>" % (self.full_path, self.id)


class KeywordAssociation(Base):
    r"""Keyword association

    Association between keyword and entry.

    Attributes
    ----------
    keyword_id : int
        Unique ID of the `metacatalog.models.Keyword`
    entry_id : int
        Unique ID of the `metacatalog.models.Entry`
    
    """
    __tablename__ = 'nm_keywords_entries'

    # columns
    keyword_id = Column(Integer, ForeignKey('keywords.id'), primary_key=True)
    entry_id = Column(Integer, ForeignKey('entries.id'), primary_key=True)

    # relationships
    keyword = relationship("Keyword", back_populates='tagged_entries')
    entry = relationship("Entry", back_populates='keywords')

    def __str__(self):
        return "<Entry ID=%d> tagged %s" % (self.entry.id, self.keyword.value)

