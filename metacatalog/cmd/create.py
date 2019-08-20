from metacatalog import Base
from ._util import connect


def create(args):
    # get a session to the database
    session = connect(args)

    # create all tables
    print("Creating Tables.")
    Base.metadata.create_all(session.bind)
    print("Done.")