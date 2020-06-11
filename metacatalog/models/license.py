import requests

from sqlalchemy import Column, CheckConstraint
from sqlalchemy import Integer, String, Boolean
from sqlalchemy.orm import relationship

from metacatalog.db.base import Base


ERR_TEMPLATE="""The requested resource seems to be unavailable.
[URL]: {url}

The connection timed out. There may be a problem with internet
connectivite or with the resource itself. If the problem 
persists, check the URL above to see if it is still available.
"""

class License(Base):
    """
    License represents usage information and limitations that is 
    distributed with each :class:`Entry <metacatalog.models.Entry>`.
    If the :class:`Entry <metacatalog.models.Entry>` records are 
    further grouped by a :class:`Composite <metacatalog.models.EntryGroupType>` 
    it is highly recommended to use the same License on all childs.
    metacatalog ships with a number of open data licenses and it is 
    recommended to only use existing data licenses. 

    .. versionchanged:: 0.1.9
        Either full_text or link have to be not NULL
    
    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    short_title : str
        The abbreviation of the license title (max 40 letters). Usually
        Licenses are more known under their short title, like 'GPL-3' 
    title : str
        The full title of the License agreement.
    summary : str
        Optional, but highly recommended to fill. The summary should
        give the user the most important information from the license
        in an understandable **not legal binding** manner.
        The license itself is stored as ``full_text`` or referenced
        via ``link``.
    full_text : str
        Full license text. This is the legally binding contract 
        that has to be acknowleged by the user. It specifies the 
        terms of usage for the reference data set. Instead of 
        copying the full text to metacatalog, a ``link`` to a permanent
        version of the license can be given. One of either is mandatory
        .. note::
            It is highly recommended to use existing licenses to 
            assure that they are legally correct.
    link : str
        URI link to the full license text. If possible, make sure 
        that the URI links a page of type ``'text/plain'`` to 
        return the license text in a user fiendly way.
    by_attribution : bool
        If True (default) this dataset has to be cited on usage.
    share_alike : bool
        If True (default) this dataset has to be licensed by the 
        same license on distribution.
    commercial_use : bool
        If True (default) this dataset can be used for commerical 
        purposes

    """
    __tablename__ = "licenses"
    __table_args__ = (
        CheckConstraint('NOT (full_text is NULL AND link is NULL)'),
    )

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

    def get_full_text(self) -> str:
        """
        Return the full text license code. If `full_text` is
        not given, the full license code will be loaded 
        from link.

        ..versionadded:: 0.1.9

        Raises
        ------
        connection_error : Exception
            If the link cannot be followed or does not return 
            the expected output
        
        Returns
        -------
        full_text : str
            The full license text
        """
        # if the license is stored, return
        if self.full_text is not None:
            return self.full_text
        
        # try to grab the license
        try:
            response = requests.get(self.link)
        except requests.exceptions.ConnectTimeout:
            raise RuntimeError(ERR_TEMPLATE.format(url=self.link))
        
        return response.text

    def __str__(self):
        return "%s <ID=%d>" % (self.title, self.id)
