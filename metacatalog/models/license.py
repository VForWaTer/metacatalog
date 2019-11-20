from sqlalchemy import Column
from sqlalchemy import Integer, String, Boolean
from sqlalchemy.orm import relationship

from metacatalog.db.base import Base


class License(Base):
    __tablename__ = "licenses"

    # columns
    id = Column(Integer, primary_key=True)
    short_title = Column(String(40), nullable=False)
    title= Column(String, nullable=False)
    summary = Column(String)
    full_text = Column(String)
    link = Column(String)
    by_attribution = Column(Boolean, default=True, nullable=False)
    share_alike = Column(Boolean, default=True, nullable=False)
    commercial_use = Column(Boolean, default=True, nullable=False)

    # relationships
    entries = relationship("Entry", back_populates='license')

    def __repr__(self):
        return "%s <ID=%d>" % (self.title, self.id)