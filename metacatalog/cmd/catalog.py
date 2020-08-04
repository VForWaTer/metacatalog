import json
from sqlalchemy.orm.exc import NoResultFound

from ._util import connect, cprint
from metacatalog import api


def get_uuid(args):
    # get the session
    session = connect(args)

    # get the uuid
    uuid = args.uuid
    
    if uuid is None:
        cprint(args, "You have to specify the UUID")
        exit(0)

    try:
        entity = api.get_uuid(session, uuid=uuid, not_found='raise')
    except NoResultFound as e:
        cprint(args, str(e))
        exit(0)

    # print
    if args.json:
        cprint(args, json.dumps(entity.to_dict(deep=False), indent=4))
    else:
        cprint(args, entity.full_path)

