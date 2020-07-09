import os, json

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, object_session

from metacatalog import models
from metacatalog.db.migration import check_database_version

CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.metacatalog', 'config.json')


def save_connection(connection,name=None):
    # load file
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
    else:
        config = dict()
        
    # set default name
    if name is None:
        name = 'default'
    
    # create engine section is needed
    if not 'engine' in config:
        config['engine'] = dict()

    # save connection
    config['engine'][name] = connection

    # save file
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)


def load_connection(name):
    # load file
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    if not 'engine' in config:
        return None 
    return config['engine'].get(name)


def get_connection_names():
    # load file
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)

    if not 'engine' in config:
        return []
    return list(config['engine'].keys())


def get_engine(*args, **kwargs):
    if len(args) == 1 and args[0] in get_connection_names():
        args = [load_connection(name=args[0])]

    elif 'connection_name' in kwargs:
        args = [load_connection(name=kwargs['connection_name'])]
        del kwargs['connection_name']
    
    elif len(args) == 0 and len(kwargs.keys()) == 0:
        args = [load_connection('default')]

    # set an application name
    kwargs.setdefault('connect_args', {'application_name': 'metacatalog_session'})

    # create a connection
    engine = create_engine(*args, **kwargs)

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

    # TODO with next version this has to be uncommented to use the new migration system
    """    
    # check las migration log
    try:
        check_database_version(session=session)
    except RuntimeError as e:
        # no missmatch allowed
        if not MISMATCH:
            raise e
        elif MISMATCH == 'print':
            print(str(e))    
    """

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
