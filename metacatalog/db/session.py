import os, json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

    # create a connection
    engine = create_engine(*args, **kwargs) 

    return engine
    

def get_session(*args, **kwargs):
    # TODO: check if the first argument was an engine
    # if len(args) > 0 and isinstance(args[0], Session):
    # check if engine was given as kwargs

    # else build a new engine
    engine = get_engine(*args, **kwargs)

    # create the Session class
    Session = sessionmaker(bind=engine)

    # return an instance
    return Session()
