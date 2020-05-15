"""
.. added: 0.1.6
"""
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import relationship
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
    value : str
        The actual value of this detail.

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
    value = Column(Text, nullable=False)

    # relationships
    entry = relationship("Entry", back_populates='details')

    def to_dict(self):
        """
        Return the detail as JSON enabled dict.

        """
        return dict(
            id=self.id,
            entry_id=self.entry_id,
            key=self.key,
            stem=self.stem,
            value=self.value
        ) 

    def __str__(self):
        return "%s = %s" % (self.key, self.value) 
