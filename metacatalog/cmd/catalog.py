import json
from sqlalchemy.orm.exc import NoResultFound

from ._util import connect
from metacatalog import api


def get_uuid(args):
    # get the session
    session = connect(args)

    # get the uuid
    uuid = args.uuid
    
    if uuid is None:
        print("You have to specify the UUID")
        exit(0)

    try:
        entity = api.get_uuid(session, uuid=uuid, not_found='raise')
    except NoResultFound as e:
        print(str(e))
        exit(0)

    # print
    if args.json:
        print(json.dumps(entity.to_dict(deep=False), indent=4))
    else:
        print(entity.full_path)

