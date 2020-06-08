import pytest
import os

from alembic import command
from alembic.config import Config

from ._util import connect
from metacatalog.db.migration import check_database_version
from metacatalog import BASEPATH


def check_no_version_mismatch(session):
    """
    This checks if there is no version mismatch

    """
    return check_database_version(session.bind)


def downgrade(conf):
    """
    Perform a database downgrade using alembic
    """
    command.downgrade(conf, '-1')
    return True


def upgrade(conf):
    """
    Perform a database upgrade using alembic
    """
    # the metacatalog will still raise the mismatch
    try:
        command.upgrade(conf, 'head')
    except RuntimeError:
        pass
    return True


def raise_version_mismatch(session):
    """
    Check if the expected version mismatch
    RuntimeError is raised.
    """
    with pytest.raises(RuntimeError) as excinfo:
        check_database_version(session.bind)
    
    # check the error message
    return "database is behind" in str(excinfo.value)


@pytest.mark.depends(on=['db_init'])
def test_migration():
    """
    After installing the database and initializing metacatalog
    a downgrade is performed. This causes a version mismatch 
    which will be tested and finally the database is upgraded 
    again.
    """
    # get a session
    session = connect(mode='session')

    # build alembic config
    al_config = Config(os.path.join(BASEPATH, '..', 'alembic.ini'))
    al_config.session = session

    # run single tests
    assert check_no_version_mismatch(session)
    assert downgrade(al_config)
    assert raise_version_mismatch(session)
    assert upgrade(al_config)
    assert check_no_version_mismatch(session)
