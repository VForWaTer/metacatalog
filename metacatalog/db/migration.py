"""
The migration bla

write this
"""
import os
from os.path import join as pjoin
import glob
import importlib
from datetime import datetime as dt

from sqlalchemy.orm import Session
import numpy as np

from metacatalog import BASEPATH, __version__
from .revisions import revisions as REVISIONS
ERR_TEMP = """VERSION MISMATCH!
metacatalog and the database have a version mismatch.
Don't Panic!

{msg}

run the following command to update:

    $> {cmd}

"""

REVISIONS_PATH = pjoin(BASEPATH, 'db', 'revisions')

REVISION_TEMPLATE = """\"\"\"
Metacatalog database revision
-----------------------------
date: {date}

{title}
{message}
\"\"\"
from sqlalchemy.orm import Session
from metacatalog import api, models


# define the upgrade function
def upgrade(session: Session):
    print('run update')


# define the downgrade function
def downgrade(session: Session):
    print('run downgrade')

"""

def revision(title=None, message=None):
    """
    """
    rev = get_local_head_id()
    if title is None:
        title = 'revision #%d' % (rev + 1)
    if message is None:
        message = ''

    with open(pjoin(REVISIONS_PATH, 'rev%d.py' % (rev + 1)), 'w') as f:
        f.write(REVISION_TEMPLATE.format(
            title=title, message=message, date=dt.now().isoformat()
            )
        )

    # read init
    with open(pjoin(REVISIONS_PATH, '__init__.py'), 'r') as f:
        # add to import 
        tpl = f.read()
        _t = '    rev%d,\n' % rev
        tpl = tpl.replace(_t, _t + '    rev%d,\n' % (rev + 1))

        # add to revisions
        _t = '%d: rev%d,\n' % (rev, rev)
        nrev = rev + 1
        tpl = tpl.replace(_t, _t + '    %d: rev%d,\n' % (nrev, nrev))
    
    # overwrite
    with open(pjoin(REVISIONS_PATH, '__init__.py'), 'w') as f:
        f.write(tpl)


def upgrade(session: Session, target='head'):
    """
    """
    if target == 'head':
        target = get_local_head_id()
    
    current = get_remote_head_id(session)

    while current < target:
        current += 1
        mod = REVISIONS[current]
        try:
            mod.upgrade(session)
            set_remote_head_id(session, current)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e



def downgrade(session: Session):
    """
    """
    rev_num = get_local_head_id() - 1
    if rev_num < 0:
        print("End of revision history. Can't downgrade.")
        return

    mod = REVISIONS[rev_num]

    try:
        mod.downgrade(session)
        set_remote_head_id(session, rev_num)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def get_remote_head_id(session: Session) -> int:
    """
    """
    return 1


def set_remote_head_id(session: Session, new_head_id: int):
    pass


def get_local_head_id() -> int:
    return len(REVISIONS.keys()) - 1


def check_database_version(session: Session):
    pass
