import json
import csv
import io

from ._util import connect
from metacatalog import api
from metacatalog.util.dict_functions import serialize, flatten

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

    # switch the output
    if args.json:
        obj = [serialize(r) for r in results]
        print(json.dumps(obj, indent=4))
    elif args.csv:
        obj = [flatten(serialize(r)) for r in results]
        f = io.StringIO(newline='')
        colnames = set([n for o in obj for n in o.keys()])
        writer = csv.DictWriter(f, fieldnames=colnames, quotechar='"', quoting=csv.QUOTE_NONNUMERIC, lineterminator='\r')
        writer.writeheader()
        for o in obj:
            writer.writerow(o)
        print(f.getvalue())
    else:   # stdOut
        for result in results:
            print(result)
