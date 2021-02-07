"""

**THE MIGRATION MODULE IS ONLY USEFUL FOR DEVELOPERS**

"""
from ._util import connect, cprint
from metacatalog.db import migration


def migrate(args):
    action = args.action

    if action == 'revision':
        revision(args)
    elif action == 'upgrade':
        upgrade(args)
    elif action == 'downgrade':
        downgrade(args)
    elif action == 'head':
        current_head(args)
    else:
        cprint(args, "Sorry, '%s' is nothing metacatalog can do for you." % action)

def revision(args):
    msg = args.message
    title = args.title

    migration.revision(title=title, message=msg)


def upgrade(args):
    session = connect(args)

    migration.upgrade(session=session)

def downgrade(args):
    session = connect(args)

    migration.downgrade(session=session)

def current_head(args):
    session = connect(args)

    head = migration.get_remote_head_id(session)

    cprint(args, 'Current database revision: %d' % head)