"""
The config module is used to define a GlobalConfig module,
which can be used to store settings and migration history
in the database.
"""
from datetime import datetime as dt
from sqlalchemy import Column
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import Session

from metacatalog.db.base import Base


class MetaCodes(type):
    @property
    def migration(self):
        return 1
    
    @property
    def error(self):
        return 99
    
    @property
    def warning(self):
        return 10

    @property
    def info(self):
        return 5

class LogCodes(metaclass=MetaCodes):
    pass


class Log(Base):
    """
    """
    __tablename__ = 'logs'

    id = Column(Integer, primary_key=True)
    tstamp = Column(DateTime, nullable=False, default=dt.utcnow)
    code = Column(Integer, nullable=False)
    description = Column(String, nullable=False)
    migration_head = Column(Integer, nullable=True)

    @classmethod
    def load_migration_head(cls, session: Session) -> int:
        query = session.query(Log.migration_head).filter(Log.code==LogCodes.migration).order_by(Log.tstamp.desc())

        return query.limit(1).scalar()

    @classmethod
    def load_last_incident(cls, session: Session, code: int=None):
        query = session.query(Log)
        if code is not None:
            query = query.filter(Log.code==code)
        query= query.order_by(Log.tstamp.desc())
        return query.first()

    @classmethod
    def load_incident(cls, session: Session, id: int):
        query = session.query(Log).filter(Log.id==id)

        return query.one()

    @classmethod
    def load_last(cls, session: Session, n=5, code: int =None):
        query = session.query(Log)
        if code is not None:
            query = query.filter(Log.code==code)
        query = query.order_by(Log.tstamp.desc())

        return query.limit(n).all()

