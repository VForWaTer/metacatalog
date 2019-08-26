from ._util import connect
from metacatalog.api import populate_defaults
  

def populate(args):
    # get a session to the database
    session = connect(args)

    # now, populate
    ignored = args.ignore if args.ignore is not None else []

    populate_defaults(session=session,ignore_tables=ignored)
    