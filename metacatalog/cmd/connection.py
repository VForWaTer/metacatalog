from metacatalog.db.session import (
    get_connection_names,
    save_connection,
    load_connection
)

from ._util import cprint

def connection(args):
    # check if the save flag is given
    if args.save is not None:
        conn = args.save
        name = args.name if args.name is not None else 'default'
        save_connection(conn, name=name)

        cprint(args, 'Saved connection information as %s' % name)
    else:
        # otherwise the saved connections should be shown
        if args.name is not None:
            cprint(args, load_connection(args.name))
        else:
            connections = get_connection_names()
            cprint(args, 'Found %d connections:' % (len(connections)))
            cprint(args, '\n'.join(connections))
