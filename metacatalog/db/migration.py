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
from metacatalog import models
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
    print('Current head [%d]' % rev)
    if title is None:
        title = 'revision #%d' % (rev + 1)
    if message is None:
        message = ''

    rev_file = pjoin(REVISIONS_PATH, 'rev%d.py' % (rev + 1))
    with open(rev_file, 'w') as f:
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
    
    print('Edit the revision file: %s' % rev_file)


def upgrade(session: Session, target='head'):
    """
    """
    if target == 'head':
        target = get_local_head_id()
    
    current = get_remote_head_id(session)
    print('Current head [%d] targeting [%d]' % (current, target))

    if current < target:
        print('Migrating [%d] ->' % current, end='')
    else:
        print('Nothing to update -> ', end='')
    while current < target:
        current += 1
        mod = REVISIONS[current]
        try:
            mod.upgrade(session)
            set_remote_head_id(session, current)
            session.commit()
            print(' [%d] ->' % current, end='')
        except Exception as e:
            session.rollback()
            raise e

    print(' done.')



def downgrade(session: Session):
    """
    """
    # here we actually use the local head, as we need the downgrade of the current head
    rev_num = get_local_head_id()
    new_rev_num = rev_num - 1
    if rev_num == 0:
        print("With metacatalog==0.2 the migration system was rebuild.\nTo downgrade to metacatalog < 0.2, use alembic as\nmigration system.")
        return

    # load the current revision module
    mod = REVISIONS[rev_num]

    try:
        # run the downgrade
        mod.downgrade(session)
        set_remote_head_id(session, new_rev_num)
        session.commit()
        print('Downgraded [%d] -> [%d]' % (rev_num, new_rev_num))
    except Exception as e:
        session.rollback()
        raise e


def get_remote_head_id(session: Session) -> int:
    """
    """
    head = models.Log.load_migration_head(session=session)
    if head is None:
        head = 0
    return head


def set_remote_head_id(session: Session, new_head_id: int, description=None):
    if description is None:
        description = 'Migrated database to %d using metacatalog==%s' % (new_head_id, __version__)
    log = models.Log(code=models.LogCodes.migration, description=description, migration_head=new_head_id)
    session.add(log)


def get_local_head_id() -> int:
    return len(REVISIONS.keys()) - 1


def check_database_version(session: Session):
    local_head = get_local_head_id()
    remote_head = get_remote_head_id(session)

    # if heads are the same, return True
    if local_head==remote_head:
        return True

    if local_head > remote_head:
        msg = 'Your database is behind the metacatalog version and needs to be updated.'
        cmd = 'python -m metacatalog migrate upgrade'
    else:
        msg = 'Your metacatalog is outdated and needs to be updated.'
        cmd = 'pip install --upgrade metacatalog'
    
    raise RuntimeError(ERR_TEMP.format(msg=msg, cmd=cmd))       
