import pytest
import logging
from sqlalchemy.orm.exc import NoResultFound

from metacatalog import api, models
from ._util import connect


def log_every_level(session):
    # get a logger
    logger = api.get_logger(session)

    # set loglevel to debug
    logger.setLevel(10)

    logger.info('First info message')
    logger.warning('Serious warning')
    logger.error('Critical error')

    # check existance
    msg = session.query(models.Log).filter(models.Log.description == 'First info message').one()
    assert msg.code == models.LogCodes.info

    msg = session.query(models.Log).filter(models.Log.description == 'Serious warning').one()
    assert msg.code == models.LogCodes.warning

    msg = session.query(models.Log).filter(models.Log.description == 'Critical error').one()
    assert msg.code == models.LogCodes.error

    return True


def log_with_loglevel(session):
    logger = logging.getLogger('metacatalog')
    logger.setLevel(40)

    # log info, which should be ignored
    logger.info('Ingore me')
    logger.error('Second error')

    # check existance
    with pytest.raises(NoResultFound):
        session.query(models.Log).filter(models.Log.description == 'Ignore me').one()

    # check existance
    msg = session.query(models.Log).filter(models.Log.description == 'Second error').one()
    assert msg.code == models.LogCodes.error

    return True


@pytest.mark.depends(on=['db_init'], name='logging')
def test_logging():
    """
    Test the logging module.
    This basic test just logs different messeages to the DB
    and then checks for their existence
    """
    # get a session
    session = connect(mode='session')

    # run tests
    assert log_every_level(session)
    assert log_with_loglevel(session)
