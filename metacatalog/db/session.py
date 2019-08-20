from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_engine(*args, **kwargs):
    # TODO: here, search for a file with the last used connection or a default connection?
    return create_engine(*args, **kwargs)

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
