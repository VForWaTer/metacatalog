from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship, Session
from sqlalchemy.dialects.postgresql import ARRAY

from metacatalog.db.base import Base


class Unit(Base):
    r"""
    Model to represent units.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    name : str
        Full name of the Unit
    symbol : str
        A max. 12 letter symbol that is **commonly** used to represent the
        unit
    si : str
        Optional. If applicable, the conversion if the unit into SI units.
        If the unit is i.e. m/km the si would be m*1000^-1*m^-1
    variables : list
        Lazy loaded list of Variables that use the current unit

    """
    __tablename__ = "units"

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    symbol = Column(String(12), nullable=False)
    si = Column(String(), nullable=True)

    # relationships
    variables = relationship("Variable", back_populates='unit')

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
            name=self.name,
            symbol=self.symbol
        )

        # set optionals
        for attr in ('si'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr)

        # lazy loading
        if deep:
            d['variables'] = [v.to_dict(deep=False) for v in self.variables]

        return d

    def __str__(self) -> str:
        return "%s <ID=%d>" % (self.name, self.id)


class Variable(Base):
    r"""
    Model to represent variables. The variable is any kind of oberservation,
    that can be represented by one data type. metacatalog does not take the
    definition of variables too strict. It is however common to keep variables
    as atomic as possbile.

    However, technically, you can also create a new variable that describes a
    combined data type and reference a newly created table via
    `DataSource <metacatalog.models.DataSource>`. This can make sense if in the
    scope and context of the metacatalog installation a sensor like a Decagon
    5TE always records three parameters at a time like Temperature, Moisture
    and Conductance. That can be implemented as a new '5TE' variable and the
    datasource would point to a table containing all three measurements.
    **Note that this should not be common practice and will make your
    metadata unusable in other contexts**.

    Attributes
    ----------
    id : int
        Unique id of the record. If not specified, the database will assign it.
    name : str
        Full name of the Unit
    symbol : str
        A max. 12 letter symbol that is **commonly** used to represent the
        unit
    si : str
        Optional. If applicable, the conversion if the unit into SI units.
        If the unit is i.e. m/km the si would be m*1000^-1*m^-1
    variables : list
        Lazy loaded list of Variables that use the current unit
    column_names : list
          .. versionadded:: 0.3.0
          List of default column names that will be displayed when exporting the data.
          The columns are named in the same order as they appear in the list.

    """
    __tablename__ = 'variables'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    symbol = Column(String(12), nullable=False)
    column_names = Column(ARRAY(String(128)), nullable=False)
    unit_id = Column(Integer, ForeignKey('units.id'), nullable=False)
    keyword_id = Column(Integer, ForeignKey('keywords.id'))

    # relationships
    entries = relationship("Entry", back_populates='variable')
    unit = relationship("Unit", back_populates='variables')
    keyword = relationship("Keyword")

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
            name=self.name,
            symbol=self.symbol,
            unit=self.unit.to_dict(deep=False),
            column_names=self.column_names
        )

        # set optionals
        for attr in ('keyword'):
            if hasattr(self, attr) and getattr(self, attr) is not None:
                d[attr] = getattr(self, attr).to_dict(deep=False)

        # lazy loading
        if deep:
            d['entries'] = [e.to_dict() for e in self.entries]

        return d

    @classmethod
    def from_dict(cls, data: dict, session: Session) -> 'Variable':
        """From dict

        Create a new Variable from a python dictionary.

        Parameters
        ----------
        data : dict
            The dictionary containing the data
        session : Session
            The database session

        Returns
        -------
        obj : Variable
            The new Variable

        """
        # check if the data has an ID
        from metacatalog import api
        if 'id' in data:
            return api.find_variable(session, id=data['id'], return_iterator=True).one()

        # check the unit data
        unit_data = data.get('unit', {})
        if 'id' in unit_data:
            unit_id = unit_data['id']
        elif len(unit_data) > 0:
            # add the unit
            unit = Unit(**unit_data)
            try:
                session.add(unit)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
            
            # ge the unit
            unit_id = unit.id
        else:
            raise ValueError("Unit data is missing")
        
        if isinstance(data['column_names'], list):
            column_names = data['column_names']
        elif isinstance(data['column_names'], str):
            column_names = [data['column_names']]

        # create new object
        variable = cls(
            name=data['name'],
            symbol=data['symbol'],
            unit_id=unit_id,
            column_names=column_names
        )

        # create the variable
        try:
            session.add(variable)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

        return variable
        
    def __str__(self) -> str:
        return "%s [%s] <ID=%d>" % (self.name, self.unit.symbol,self.id)
