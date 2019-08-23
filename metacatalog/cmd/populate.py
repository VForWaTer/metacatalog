from ._util import connect
from metacatalog.api import populate_defaults
  

def populate(args):
    # get a session to the database
    session = connect(args)

    # now, populate
    # TODO get ignored tables from args
    ignored = []

    populate_defaults(session=session,ignore_tables=ignored)
    