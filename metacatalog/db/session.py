from typing import Optional
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from metacatalog import models
from metacatalog.config import config


def get_engine(connection: Optional[str] = None, **kwargs):
    # check if a connection is given
    url = connection or str(config.connection)

    # set an application name
    kwargs.setdefault('connect_args', {'application_name': 'metacatalog_session'})

    # create a connection
    engine = create_engine(url, **kwargs)

    return engine
    

def get_session(*args, **kwargs):
    # TODO: check if the first argument was an engine
    # if len(args) > 0 and isinstance(args[0], Session):
    # check if engine was given as kwargs

    # check the version
    if 'version_mismatch' in kwargs and kwargs['version_mismatch']:
        MISMATCH = kwargs['version_mismatch']
        del kwargs['version_mismatch']
    else:
        MISMATCH = False

    # else build a new engine
    engine = get_engine(*args, **kwargs)

    # create the Session class
    Session = sessionmaker(bind=engine)

    # create the instance
    session = Session()

    # hook up some event listeners
    @event.listens_for(session, 'before_flush')
    def update_keywords_full_path(session, context, instances):
        # new  Keyword instances get updated
        for instance in session.new:
            # just look for Keywords
            if isinstance(instance, models.Keyword):
                instance.full_path = instance.path()
                session.add(instance)
        # TODO: if keywords in session.dirty, it was a update and other keywords might need an update as,well

    # @event.listens_for(session, 'after_flush')
    # def insert_latest_entry_version_number(session, context):
    #     for instance in session.new:
    #         if isinstance(instance, Entry):
    #             if instance.latest_version_id is None:
    #                 instance.latest_version_id = instance.id


    # return an instance
    return session
