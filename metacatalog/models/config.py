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
        return 40
    
    @property
    def warning(self):
        return 30

    @property
    def info(self):
        return 20
    
    @property
    def debug(self):
        return 10

    @classmethod
    def level_name(cls, code: int) -> str:
        if code >= 40:
            return 'ERROR'
        elif code >= 30:
            return 'WARNING'
        elif code >= 20:
            return 'INFO'
        elif code > 1:
            return 'DEBUG'
        else:
            return 'MIGRATION'


class LogCodes(metaclass=MetaCodes):
    pass


class Log(Base):
    """
    """
    __tablename__ = 'logs'

    id = Column(Integer, primary_key=True)
    tstamp = Column(DateTime, nullable=False, default=dt.utcnow)
    code = Column(Integer, nullable=False)
    code_name = Column(String(20), nullable=False)
    description = Column(String, nullable=False)
    migration_head = Column(Integer, nullable=True)

    def __init__(self, **kwargs):
        if 'code' in kwargs:
            kwargs['code_name'] = LogCodes.level_name(kwargs['code'])
        super(Log, self).__init__(**kwargs)

    @classmethod
    def load_migration_head(cls, session: Session) -> int:
        query = session.query(Log.migration_head).filter(Log.code==LogCodes.migration).order_by(Log.tstamp.desc())

        # This often blocks the logs table, no idea why, thus it 
        # is now comitting the session.
        head = query.limit(1).scalar()
        session.commit()

        return head

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

    def __str__(self):
        return f"[{self.code_name}]: {self.description} ({self.tstamp.isoformat()})"
