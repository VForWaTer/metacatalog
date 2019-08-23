from metacatalog.api import create_tables
from ._util import connect


def create(args):
    # get a session to the database
    session = connect(args)

    # create all tables
    print("Creating Tables.")
    create_tables(session=session)
    print("Done.")