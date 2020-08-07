from metacatalog.api import create_tables
from ._util import connect, cprint


def create(args):
    # get a session to the database
    session = connect(args)

    # create all tables
    cprint(args, "Creating Tables...")
    create_tables(session=session)
    cprint(args, "Done.")