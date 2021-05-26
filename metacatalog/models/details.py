"""
.. added: 0.1.6
"""
from typing import Union
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import JSONB
from metacatalog.db.base import Base


class Detail(Base):
    """Entry detail

    `Detail` data are optional key-value pairs that can be linked to
    `metacatalo.models.Entry` records by `1:n` relationships. This is
    vital metadata information that is specific to the `Entry` itself.
    E.g. specific to the sensor or variable, but cannot be generalized
    to all kinds of `Entry` types.

    Details can be loaded as a python dict or be converted to
    text-based tables. A HTML or markdown table can e.g. be appended
    to the `Entry.abstract` on export.

    Since version 0.1.13, it is possible to link an existing
    :class:`Thesaurus <metacatalog.models.Thesaurus>` to the detail.
    This makes the export to ISO 19115 in princile possible as an
    ``MD_MetadataExtensionInformation`` object.

    Attributes
    ----------
    id : int
        Primary Key. Identifies the record. If left empty the
        Database will assign one.
    entry_id : int
        Foreign Key. Identifies the `metacatalog.models.Entry`
        which is decribed by this detail.
    key : str
        The key of the key vaule par. Maximum 20 letters,
        ideally no whitespaces.
    stem : str
        Stemmed key using a `nltk.PorterStemmer`. The stemmed
        key can be used to search for related keys
    value : str, list
        .. versionchanged:: 0.3.0
        The actual value of this detail. This can be a string
        or a flat dictionary.
    description : str
        Description what the key means in the context of the
        :class:`Entry <metacatalog.models.Entry>` or
        :class:`EntryGroup <metacatalog.models.EntryGroup>`. Optional,
        can be omitted, if not applicable.
    thesaurus : metacatalog.models.Thesaurus
        .. versionadded:: 0.1.13
        Optional. If the detail :attr:`key` is described in a thesaurus or
        controlled dictionary list, you can link the thesaurus
        to the detail. Details with thesaurus information are
        in principle exportable to ISO 19115 using an
        ``MD_MetadataExtensionInformation``.
    thesaurus_id : int
        .. versionadded:: 0.1.13
        Foreign key of the linked
        :class:`Thesaurus <metacatalog.models.Thesaurus>`.

    """
    __tablename__ = 'details'
    __table_args__ = (
        UniqueConstraint('entry_id', 'stem'),
    )

    # columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    entry_id = Column(Integer, ForeignKey('entries.id'))
    key = Column(String(20), nullable=False)
    stem = Column(String(20), nullable=False)
    raw_value = Column(MutableDict.as_mutable(JSONB), nullable=False)
    description = Column(String, nullable=True)
    thesaurus_id = Column(Integer, ForeignKey('thesaurus.id'))

    # relationships
    entry = relationship("Entry", back_populates='details')
    thesaurus = relationship("Thesaurus")

    def __init__(self, **kwargs):
        # handle values, if given
        if 'value' in kwargs:
            value = kwargs['value']
            del kwargs['value']

        # call the main init func
        super(Detail, self).__init__(**kwargs)

        # set the value via the property
        self.value = value

    @property
    def value(self):
        if '__literal__' in self.raw_value:
            return self.raw_value.get('__literal__')
        else:
            return dict(self.raw_value)

    @value.setter
    def value(self, new_value: Union[str, dict]):
        if not isinstance(new_value, dict):
            new_val = {'__literal__': new_value}
        else:
            new_val = new_value
        self.raw_value = new_val

    def to_dict(self, deep=False):
        """
        Return the detail as JSON enabled dict.

        """
        d = dict(
            id=self.id,
            key=self.key,
            stem=self.stem,
            value=self.value,
        )

        if self.description is not None:
            d['description'] = self.description

        if deep:
            d['entry'] = self.entry.to_dict(deep=False)
        else:
            d['entry_id'] = self.entry.id
            d['entry_uuid'] = self.entry.uuid

        return d

    def __str__(self):
        if self.thesaurus is not None:
            return '%s = %s <%s>' % (self.key, self.value, self.thesaurus.name)
        else:
            return "%s = %s" % (self.key, self.value)
