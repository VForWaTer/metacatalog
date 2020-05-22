from ._util import connect
from metacatalog import api
from metacatalog.api._mapping import ADD_MAPPING


def add(args):
    # get the session
    session = connect(args)

    # get the parameters
    entity = args.entity.lower()

    if args.csv is not None:
        records = api.from_csv(args.csv)
    elif args.txt is not None:
        records = api.from_text(args.txt)
    elif args.json is not None:
        records = api.from_json(args.json)
    else:
        print("You need to set the data origin flag. One of ['--csv', '--txt', '--json']")
        exit(1)
    
    # is entity supported?
    if entity.lower() not in ADD_MAPPING.keys():
        print('Only adding [%s] by API is supported as of now' % (','.join(ADD_MAPPING.keys())))
        exit(1)

    # add
    add_func = ADD_MAPPING.get(entity.lower())

    # check if only one entity was added
    if not isinstance(records, (list, tuple)):
        records = [records]

    try:
        for record in records:
            add_func(session, **record)
    except Exception as e:
        print('Something went wrong.\n\nError message:\n%s\n' % str(e))
        print('API help says:\n%s' % add_func.__doc__)
        exit(1)

    print('Added %d %s records.\nDone.' % (len(records), entity))