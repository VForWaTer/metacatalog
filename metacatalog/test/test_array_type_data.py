import pytest

import pandas as pd
import numpy as np
from numpy.testing import assert_array_almost_equal
from metacatalog import api
from ._util import connect



def add_3D_entry(session):
    """
    Add an entry for the eddy wind data.
    """
    # add the variable
    var_3D_wind = api.add_variable(session, name='3D-wind', symbol='uvw', column_names=['u', 'v', 'w'], unit=107)

    # add an author
    kit = api.add_person(session, first_name=None, last_name=None,
                         organisation_name='Karlsruhe Institute of Technology (KIT)',
                         organisation_abbrev='KIT'
                        )

    # add the entry
    entry_3D_wind = api.add_entry(session, title='3-dimensional windspeed data',
                              abstract='3-dimensional windspeed data from the Fendt data set',
                              location=(8, 52),
                              variable=var_3D_wind.id,
                              comment='after double rotation',
                              license=6,
                              author=kit.id,
                              embargo=False,
                              is_partial=False)

    # assert
    assert var_3D_wind.column_names == ['u', 'v', 'w']

    return True


def create_3D_datasource(session, df_3D_wind):
    """
    Add a datasource to the eddy entry.
    """
    entry_3D_wind = api.find_entry(session, title='3-dimensional windspeed data')[0]
    entry_3D_wind.create_datasource(type=1, path='timeseries', datatype='timeseries')

    entry_3D_wind.datasource.create_scale(resolution='30min', extent=(df_3D_wind.index[0], df_3D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    session.commit()

    # assert
    assert entry_3D_wind.variable.column_names == ['u', 'v', 'w']

    return True


def add_3D_data(session, df_3D_wind):
    """
    Add Eddy 3D windspeed data to the eddy entry.
    """
    entry_3D_wind = api.find_entry(session, title='3-dimensional windspeed data')[0]
    entry_3D_wind.import_data(df_3D_wind)

    return True


def read_3D_data(session):
    """
    Read the 3D windspeed data and check column names.
    """
    entry_3D_wind = api.find_entry(session, title='3-dimensional windspeed data')[0]

    dat = entry_3D_wind.get_data()

    # assert
    assert dat.columns[1] == 'v'
    assert dat.columns.tolist() == ['u', 'v', 'w']
    assert dat.index[2] == pd.to_datetime("2018-01-01 01:30:00", format='%Y-%m-%d %H:%M:%S')
    assert dat['u'].mean() == pytest.approx(3.1, 0.05)

    return True


def one_dim_data(session, df_1D_wind):
    """
    Do the same as above, but with one-dimensional data instead.
    """
    # add the variable
    var_1D_wind = api.add_variable(session, name='1D-wind', symbol='u', column_names=['u'], unit=107)

    # find the previously added author
    kit = api.find_person(session, organisation_abbrev='KIT')[0]

    # add the entry
    entry_1D_wind = api.add_entry(session, title='1-dimensional windspeed data',
                                  abstract='1-dimensional windspeed data from the Fendt data set',
                                  location=(8, 52),
                                  variable=var_1D_wind.id,
                                  license=6,
                                  author=kit.id,
                                  embargo=False,
                                  is_partial=False)

    # create datasource and scale
    entry_1D_wind.create_datasource(type=1, path='timeseries', datatype='timeseries')

    entry_1D_wind.datasource.create_scale(resolution='30min', extent=(df_1D_wind.index[0], df_1D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    # add data
    entry_1D_wind.import_data(df_1D_wind)

    # read data
    dat = entry_1D_wind.get_data()

    # assert
    assert dat.columns == 'u'
    assert dat['u'].mean() == pytest.approx(3.1, 0.05)

    return True


def force_data_names_true(session, df_3D_wind):
    """
    Test force_data_names=True when loading the data into the database.
    In this case, datasource.data_names will be overwritten with the column
    names of the imported data, when exporting the data, these column col_names
    will be displayed.
    We use the 3D eddy wind data for this again.
    """
    # find the variable
    var_3D_wind = api.find_variable(session, name='3D-wind')[0]

    # find the previously added author
    kit = api.find_person(session, organisation_abbrev='KIT')[0]

    # add the entry
    entry_3D_force_data_names = api.add_entry(session, title='3-dimensional windspeed data, force_data_names',
                                              abstract='3-dimensional windspeed data from the Fendt data set',
                                              location=(8, 52),
                                              variable=var_3D_wind.id,
                                              comment='after double rotation',
                                              license=6,
                                              author=kit.id,
                                              embargo=False,
                                              is_partial=False)

    # create datasource and scale
    entry_3D_force_data_names.create_datasource(type=1, path='timeseries', datatype='timeseries')

    entry_3D_force_data_names.datasource.create_scale(resolution='30min', extent=(df_3D_wind.index[0], df_3D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    # add data
    entry_3D_force_data_names.import_data(df_3D_wind, force_data_names=True)

    #load data
    dat = entry_3D_force_data_names.get_data()

    # assert
    assert dat.columns.tolist() == ['u_ms', 'v_ms', 'w_ms']
    assert dat['u_ms'].mean() == pytest.approx(3.1, 0.05)

    return True

def precision_test(session, df_3D_wind, df_3D_prec):
    """
    Test if precision columns are handled correctly.
    We use the 3D eddy wind data with 3 precision columns for this.
    """
    # find the variable
    var_3D_wind = api.find_variable(session, name='3D-wind')[0]

    # find the previously added person
    kit = api.find_person(session, organisation_abbrev='KIT')[0]

    # add the entry
    entry_3D_precision = api.add_entry(session, title='3-dimensional windspeed data, precision',
                                       abstract='3-dimensional windspeed data from the Fendt data set',
                                       location=(8, 52),
                                       variable=var_3D_wind.id,
                                       comment='after double rotation',
                                       license=6,
                                       author=kit.id,
                                       embargo=False,
                                       is_partial=False)

    # create datasource and scale
    entry_3D_precision.create_datasource(type=1, path='timeseries', datatype='timeseries')

    entry_3D_precision.datasource.create_scale(resolution='30min', extent=(df_3D_wind.index[0], df_3D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    # add data
    entry_3D_precision.import_data(data=df_3D_wind, precision=df_3D_prec, force_data_names=False)

    #load data
    dat = entry_3D_precision.get_data()

    # assert
    assert dat.columns.tolist() == ['u', 'v', 'w', 'precision1', 'precision2', 'precision3'] # note: input was 'precision_1'
    assert dat['u'].mean() == pytest.approx(3.1, 0.05)

    return True

def auto_force_data_names(session, df_1D_wind, df_3D_prec):
    """
    If len(data_columns) != len(entry.variable.column_names) force_data_names
    should automatically become True and the column names of the imported data
    should be saved in datasource.data_names.
    To test this, we add 1D wind data (with 3D precision) to the 3D wind
    variable with variable.column_names=['u', 'v', 'w'].
    """
    # find the variable
    var_3D_wind = api.find_variable(session, name='3D-wind')[0]

    # find the previously added person
    kit = api.find_person(session, organisation_abbrev='KIT')[0]

    # add the entry
    entry_1D_precision = api.add_entry(session, title='1-dimensional windspeed data, precision',
                                       abstract='1-dimensional windspeed data',
                                       location=(8, 52),
                                       variable=var_3D_wind.id,
                                       comment='after double rotation',
                                       license=6,
                                       author=kit.id,
                                       embargo=False,
                                       is_partial=False)
    # create datasource and scale
    entry_1D_precision.create_datasource(type=1, path='timeseries', datatype='timeseries')

    entry_1D_precision.datasource.create_scale(resolution='30min', extent=(df_1D_wind.index[0], df_1D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    # add data
    entry_1D_precision.import_data(data=df_1D_wind, precision=df_3D_prec, force_data_names=False)

    #load data
    dat = entry_1D_precision.get_data()

    # assert
    assert dat.columns.tolist() == ['u_ms', 'precision1', 'precision2', 'precision3']
    assert dat['u_ms'].mean() == pytest.approx(3.1, 0.05)

    return True


def add_split_dataset(session):
    # create dummy data
    data = pd.DataFrame(data={'value': np.random.normal(10, 1, size=350), 'tstamp': pd.date_range('201309241100', periods=350, freq='15min')})
    data.set_index('tstamp', inplace=True)

    # add two entries as split datasets
    kit = api.find_person(session, organisation_abbrev='KIT')[0]
    historical_entry = api.add_entry(session, title='Historical data', abstract='Long descirption', location=(4.2, 42), variable=1, license=6, author=kit.id)
    recent_entry = api.add.add_entry(session, title='Recent data', abstract='something bad happended that needs description', location=(4.2, 42), variable=1, license=6, author=kit.id)
    
    # create datasource
    historical_entry.create_datasource(type=1, path='timeseries', datatype='timeseries')
    recent_entry.create_datasource(type=1, path='timeseries', datatype='timeseries')
    
    # split the data
    historical_entry.import_data(data=data.iloc[:300, :])
    recent_entry.import_data(data=data.iloc[300:, :])
    
    full_dataset = api.add_group(session, 'Split dataset', [historical_entry.id, recent_entry.id])

    # checkout
    result = api.find_entry(session, id=recent_entry.id, as_result=True)[0]

    # recover data
    db_data = result.get_data()

    # search for checksum - result.checksum is a checksum of member checksum, which is only one here
    assert len(result.checksums) == 1
    checksum = result.checksums[0]
    assert checksum in db_data
    
    recovered_data = db_data[checksum].values
    assert_array_almost_equal(data.values, recovered_data)

    # Split datasets are concatenated row-wise (pd.concat) -> only one column
    assert db_data[checksum].shape == (350, 1)

    return True

def add_composite_dataset(session):
    # create dummy data
    data_left = pd.DataFrame(data={'value': np.random.normal(10, 1, size=350), 'tstamp': pd.date_range('201309241100', periods=350, freq='15min')})
    data_left.set_index('tstamp', inplace=True)

    data_right = pd.DataFrame(data={'value': np.random.normal(10, 1, size=352), 'tstamp': pd.date_range('201309241100', periods=352, freq='15min')})
    data_right.set_index('tstamp', inplace=True)

    # add two entries as split datasets
    kit = api.find_person(session, organisation_abbrev='KIT')[0]
    entry_left = api.add_entry(session, title='Left column data', abstract='Left data for merging', location=(4.2, 42), variable=1, license=6, author=kit.id)
    entry_right = api.add.add_entry(session, title='Right column data', abstract='Right data for merging', location=(4.2, 42), variable=2, license=6, author=kit.id)
    
    # create datasource
    entry_left.create_datasource(type=1, path='timeseries', datatype='timeseries')
    entry_right.create_datasource(type=1, path='timeseries', datatype='timeseries')
    
    # split the data
    entry_left.import_data(data=data_left)
    entry_right.import_data(data=data_right)
    
    full_dataset = api.add_group(session, 'Composite', [entry_left.id, entry_right.id])

    # checkout
    result = api.find_entry(session, id=entry_right.id, as_result=True)[0]

    # recover data
    db_data = result.get_data()
    
    # Composite datasets are merged column-wise (pd.merge) -> keep columns and maximal number of rows
    assert db_data.shape == (352, 2)
    assert list(db_data.columns) == ['air_temperature', 'soil_temperature']

    return True


@pytest.mark.depends(on=['db_init'], name='array_type_data')
def test_array_type_data():
    """
    Test if timeseries array works correctly.
    Backward compatibility with the old timeseries path is tested in test_models_data.py
    """
    # get a session
    session = connect(mode='session')

    # using eddy wind speed data for the tests (u, v, w)
    tstamp = "2018-01-01 00:30:00", "2018-01-01 01:00:00", "2018-01-01 01:30:00", "2018-01-01 02:00:00", "2018-01-01 02:30:00", "2018-01-01 03:00:00", "2018-01-01 03:30:00", "2018-01-01 04:00:00", "2018-01-01 04:30:00", "2018-01-01 05:00:00"
    u = 1.123902, 0.214753, 0.446611, 0.962977, 2.915902, 4.048897, 5.368552, 6.046246, 5.405221, 4.172279
    v = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 , 0.0
    w = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 , 0.0
    prec1 = np.random.rand(10)
    prec2 = np.random.rand(10)
    prec3 = np.random.rand(10)

    # generate 3D data
    df_3D_wind = pd.DataFrame(data={"tstamp": tstamp, "u_ms": u, "v_ms": v, "w_ms": w})    # use different column names to test force_data_names=True
    df_3D_wind['tstamp'] = pd.to_datetime(df_3D_wind['tstamp'], format='%Y-%m-%d %H:%M:%S')
    df_3D_wind.set_index('tstamp', inplace=True)

    # generate 1D data
    df_1D_wind = pd.DataFrame(data={"tstamp": tstamp, "u_ms": u})
    df_1D_wind['tstamp'] = pd.to_datetime(df_1D_wind['tstamp'], format='%Y-%m-%d %H:%M:%S')
    df_1D_wind.set_index('tstamp', inplace=True)

    # generate 3D data with random 3D precision
    df_3D_prec = pd.DataFrame(data={"tstamp": tstamp, "precision_1": prec1, "precision_2": prec2, "precision_3": prec3})
    df_3D_prec['tstamp'] = pd.to_datetime(df_3D_prec['tstamp'], format='%Y-%m-%d %H:%M:%S')
    df_3D_prec.set_index('tstamp', inplace=True)

    # use a copy of precision for auto_force_data_names()
    df_3D_prec_copy = df_3D_prec.copy()


    # run single tests
    assert add_3D_entry(session)
    assert create_3D_datasource(session, df_3D_wind)
    assert add_3D_data(session, df_3D_wind)
    assert read_3D_data(session)
    assert one_dim_data(session, df_1D_wind)
    assert force_data_names_true(session, df_3D_wind)
    assert precision_test(session, df_3D_wind, df_3D_prec)
    assert auto_force_data_names(session, df_1D_wind, df_3D_prec_copy)
    assert add_split_dataset(session)
    assert add_composite_dataset(session)
