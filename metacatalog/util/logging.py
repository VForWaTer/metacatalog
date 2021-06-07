"""
Logging to database
-------------------

metacatalog is capable of streaming any log event into the database,
which is emitted using the builtin :any:`logging` module.
This is convenient if you build some logic on the API or Models and
want to persist warnings and critical errors caused by the users
into the database.
You need to pass a session created by 
:func:`connect_database <metacatalog.api.connect_database>` to the
logger, to enable it.

"""
import logging
import sqlalchemy as sa
from datetime import datetime as dt

from metacatalog.models import Log, LogCodes


class LogHander(logging.Handler):
    def __init__(self, session: sa.orm.Session):
        # initialize the base handler
        logging.Handler.__init__(self)

        # store the session
        self.session = session
    
    def emit(self, record: logging.LogRecord):
        # create the log
        log = Log(
            tstamp = dt.fromtimestamp(record.created),
            description = record.msg,
            code=record.levelno
        )

        # store
        try:
            self.session.add(log)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e


def get_logger(session: sa.orm.Session):
    """
    Get a logger that persists to database.
    """
    # instantiate the logger
    db_logger = LogHander(session=session)

    # add logger
    logging.getLogger('metacatalog').addHandler(db_logger)

    return logging.getLogger('metacatalog')
