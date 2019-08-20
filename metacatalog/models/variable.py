from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship

from metacatalog.db import Base


class Unit(Base):
    __tablename__ = "units"

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    symbol = Column(String(12), nullable=False)

    # relationships
    variables = relationship("Variable", back_populates='unit')

    def __str__(self):
        return self.name


class Variable(Base):
    __tablename__ = 'variables'

    # columns
    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    symbol = Column(String(12), nullable=False)
    unit_id = Column(Integer, ForeignKey('units.id'), nullable=False)

    # relationships
    entries = relationship("Entry", back_populates='variable')
    unit = relationship("Unit", back_populates='variables')

    def __str__(self):
        return "%s [%s]" % (self.name, self.unit.symbol)