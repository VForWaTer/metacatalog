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
            short_title=self.short_title,
            title=self.title,
            by_attribution=self.by_attribution,
            share_alike=self.share_alike,
            commercial_use=self.commercial_use
        )

        # set optionals
        for attr in ('summary', 'full_text', 'link'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # deep loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    def __str__(self):
        return "%s <ID=%d>" % (self.title, self.id)
