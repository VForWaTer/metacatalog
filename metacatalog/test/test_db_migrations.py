import pytest
import os
import subprocess

from ._util import connect, cleanup
from metacatalog.db import migration
from metacatalog import BASEPATH


def check_no_version_mismatch(session):
    """
    This checks if there is no version mismatch

    """
    return migration.check_database_version(session)


def downgrade(session):
    """
    Perform a database downgrade
    """
    migration.downgrade(session)
    return True


def upgrade(session):
    """
    Perform a database upgrade using alembic
    """
    # the metacatalog will still raise the mismatch
    try:
        migration.upgrade(session)
    except RuntimeError:
        pass
    return True


def raise_version_mismatch(session):
    """
    Check if the expected version mismatch
    RuntimeError is raised.
    """
    with pytest.raises(RuntimeError) as excinfo:
        migration.check_database_version(session)
    
    # check the error message
    return "database is behind" in str(excinfo.value)


@pytest.mark.depends(on=['cli_find'])
def test_migration(capsys):
    """
    After installing the database and initializing metacatalog
    a downgrade is performed. This causes a version mismatch 
    which will be tested and finally the database is upgraded 
    again.
    """
    # get a session
    session = connect(mode='session')

    # run single tests
    assert check_no_version_mismatch(session)
    # something on the downgrade is not working properly.
    with capsys.disabled():
        assert downgrade(session)
    assert raise_version_mismatch(session)
    assert upgrade(session)
    assert check_no_version_mismatch(session)

    # cleanup - only if migration test is the last one
    with capsys.disabled():
        cleanup()
