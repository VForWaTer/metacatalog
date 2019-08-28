from ._util import connect
from metacatalog.api import find_unit, find_variable, find_license, find_keyword

def find(args):
    # get the session
    session = connect(args)

    # get the entity
    entity = args.entity

    # set by to an empty list if not given
    if args.by is None:
        args.by = []


    # parse out the BY arguments
    kwargs=dict()
    for by in args.by:
        # if len(by) != 2:
        kwargs[by[0]] = by[1]

    # switch entity
    if entity.lower() == 'units':
        results = find_unit(session, **kwargs)
    elif entity.lower() == 'variables':
        results = find_variable(session, **kwargs)
    elif entity.lower() == 'licenses':
        results = find_license(session, **kwargs)
    elif entity.lower() == 'keywords':
        results = find_keyword(session, **kwargs)
    else:
        print('Oops. Finding %s is not supported.' % entity)
        exit(0)

    # print out the results
    for result in results:
        print(result)
