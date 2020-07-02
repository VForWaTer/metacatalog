"""
This e2e Test needs the add-find API tests to be finished.

It will use the Entries created in that test to create 
some data samples and upload them to the database.

"""
import pytest

import pandas as pd
import numpy as np
from numpy.testing import assert_array_almost_equal

from metacatalog import models, ext
from metacatalog.ext.io import IOExtension
from ._util import connect


def create_datasource(session, entry: models.Entry, data):
    # create the datasource
    datasource = entry.create_datasource('timeseries', 'internal', 'timeseries', commit=True)
    assert datasource is not None

    # check
    assert isinstance(datasource, models.DataSource)

    # build a temporal scale
    index = data.index
    datasource.create_scale(
        resolution=index.inferred_freq,
        extent=[index[0].isoformat(), index[-1].isoformat()],
        support=1.0,
        scale_dimension='temporal'
    )

    session.add(datasource)
    session.commit()

    return True


def import_data(session, entry: models.Entry, data):
    entry.import_data(data)

    nrecords = session.execute('SELECT count(*) from timeseries where entry_id=%d' % entry.id)
    return nrecords.scalar() == 400


def read_data(session, entry, data):
    db_data = entry.get_data()

    return assert_array_almost_equal(
        getattr(db_data, entry.variable.name).values,
        data.value.values,
        decimal=3
    )


def append_data(session, entry: models.Entry, data):
    entry.append_data(data)

    nrecords = session.execute("SELECT count(*) FROM timeseries WHERE entry_id=%d" % entry.id)
    return nrecords.scalar() == 450


def delete_data(session, entry):
    entry.delete_data(delete_source=True)

    nrecords = session.execute("SELECT count(*) FROM timeseries WHERE entry_id=%d" % entry.id)
    assert nrecords.scalar() == 0

    return entry.datasource is None



@pytest.mark.depends(on=['add_find'], name='data_crud')
def test_data_crud_operations():
    """
    Use one of the test entries to perform basic CRUD
    operations
    """
    # get a session
    session = connect(mode='session')

    # find the entry to be used
    entry = session.query(models.Entry).filter(models.Entry.id==1).one()

    # build random data
    timeseries = pd.DataFrame({'tstamp': pd.date_range('201309240513', freq='15min', periods=400), 'value': np.random.normal(150, 4, size=400)})
    timeseries.set_index('tstamp', inplace=True)
    new_chunk = pd.DataFrame({'tstamp': pd.date_range('201309280913', freq='15min', periods=50), 'value': np.random.normal(0, 1, size=50)})
    new_chunk.set_index('tstamp', inplace=True)
    all_data = timeseries.append(new_chunk)

    # activate the IOExtension
    # TODO fix this in tests
    ext.extension('io', IOExtension)

    # run the tests
    assert create_datasource(session, entry, timeseries)
    assert import_data(session, entry, timeseries)
    read_data(session, entry, timeseries)
    assert append_data(session, entry, new_chunk)
    read_data(session, entry, all_data)
    assert delete_data(session, entry)