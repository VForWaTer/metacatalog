"""
The migration bla

write this
"""
import os

from sqlalchemy.engine import Engine
from alembic import script
from alembic.runtime import migration
from alembic.config import Config
from alembic.util import CommandError

from metacatalog import BASEPATH, __version__
ERR_TEMP = """VERSION MISMATCH!
metacatalog and the database have a version mismatch.
Don't Panic!

{msg}

run the following command to update:

    $> {cmd}

"""

def check_database_version(engine: Engine):
    """
    Based on: https://gist.github.com/m-aciek/118d450ee59a41176214b5f93a02cc6f
    """
    # get the alembic config file
    conf_path = os.path.abspath(os.path.join(BASEPATH, '..', 'alembic.ini'))
    print(conf_path)
    config = Config(conf_path)
    script_ = script.ScriptDirectory.from_config(config)

    # connect to database
    context = migration.MigrationContext.configure(engine.connect())
    current_revision = context.get_current_revision()
    
    # check
    if script_.get_current_head() == current_revision:
        # everything is fine
        return True
    
    # we have a version mismatch, try to figure out which
    try:
        rev = script_.get_revision(current_revision)
        # database is out of date
        msg = 'Your database is behind the current metacatalog version.\n You can use alembic to upgrade.'
        cmd = 'alembic upgrade head'
    except CommandError:
        # metacatalog is out of date
        msg = 'Your metacatalog (%s) is out of date. You need to update.' % __version__
        cmd = 'pip install --upgrade metacatalog'

    tpl = ERR_TEMP.format(msg=msg, cmd=cmd)

    raise RuntimeError(tpl)
    
