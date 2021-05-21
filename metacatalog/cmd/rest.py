from metacatalog.rest.app import app
from ._util import connect


def run(args):
    """
    Start the RESTful server hosting Metacatalog API
    """
    # append the session to app
    session = connect(args)
    app.config['session'] = session

    # run
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug
    )

