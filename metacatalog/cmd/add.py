from ._util import connect
from metacatalog import api

ADD_MAPPING = dict(
    license=api.add_license,
    licenses=api.add_license,
    keyword=api.add_keyword,
    keywords=api.add_keyword,
    unit=api.add_unit,
    units=api.add_unit,
    variable=api.add_variable,
    variables=api.add_variable,
    person=api.add_person,
    persons=api.add_person,
    author=api.add_person,
    contributors=api.add_person,
    entry=api.add_entry,
    entries=api.add_entry,
)


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