from metacatalog.db.session import (
    get_connection_names,
    save_connection,
    load_connection
)

def connection(args):
    # check if the save flag is given
    if args.save is not None:
        conn = args.save
        name = args.name if args.name is not None else 'default'
        save_connection(conn, name=name)

        print('Saved connection information as %s' % name)
    else:
        # otherwise the saved connections should be shown
        if args.name is not None:
            print(load_connection(args.name))
        else:
            connections = get_connection_names()
            print('Found %d connections:' % (len(connections)))
            print('\n'.join(connections))
