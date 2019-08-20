from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship

from metacatalog.db import Base


class License(Base):
    __tablename__ = "licenses"

    # columns
    id = Column(Integer, primary_key=True)
    title = Column(String(128), nullable=False)
    full_text = Column(String)
    link = Column(String)

    # relationships
    entries = relationship("Entry", back_populates='license')

    def __str__(self):
        return "%s <%d>" % (self.title, self.id)