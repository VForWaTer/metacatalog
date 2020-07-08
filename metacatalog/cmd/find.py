from ._util import connect
from metacatalog import api

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
        results = api.find_unit(session, **kwargs)
    elif entity.lower() == 'variables':
        results = api.find_variable(session, **kwargs)
    elif entity.lower() == 'licenses':
        results = api.find_license(session, **kwargs)
    elif entity.lower() == 'keywords':
        results = api.find_keyword(session, **kwargs)
    elif entity.lower() == 'role':
        results = api.find_role(session, **kwargs)
    elif entity.lower() == 'person':
        results = api.find_person(session, **kwargs)
    elif entity.lower() == 'group_type':
        results = api.find_group_type(session, **kwargs)
    elif entity.lower() == 'group':
        results = api.find_group(session, **kwargs)
    elif entity.lower() == 'entry':
        results = api.find_entry(session, **kwargs)
    elif entity.lower() == 'thesaurus':
        results = api.find_thesaurus(session, **kwargs)
    else:
        print('Oops. Finding %s is not supported.' % entity)
        exit(0)

    # print out the results
    for result in results:
        print(result)
