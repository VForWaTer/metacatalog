"""
`Detail` data are optional key-value pairs that can be linked to 
`Entry` records by `1:n` relationships. This is vital metadata 
information that is specific to the `Entry` itself. E.g. specific 
to the sensor or variable, but cannot be generalized to all 
kinds of `Entry` types. 

Details can be loaded as a python dict or be converted to 
text-based tables. A HTML or markdown table can e.g. be appended 
to the `Entry.abstract` on export.

"""
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import relationship
from metacatalog.db.base import Base


class Detail(Base):
    """
    """
    __tablename__ = 'details'
    __table_args__ = (
        UniqueConstraint('entry_id', 'stem')
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
            stem=self.stem,
            value=self.value
        ) 
