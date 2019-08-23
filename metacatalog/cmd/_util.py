from metacatalog import __version__ as VERSION
from metacatalog.api import connect_database

def welcome():
    print("MetaCatalog management CLI (v%s)" % VERSION)


def empty():
    welcome()
    print("Nothing to do.\nRun with -h flag to get options.")


def connect(args):
    # check if a connection string was set
    conn = args.connection if args.connection is not None else 'postgresql://postgres@localhost:5432/metacatalog'
    echo = args.verbose if args.verbose is not None else False
    
    session = connect_database(conn, echo=echo)

    return session