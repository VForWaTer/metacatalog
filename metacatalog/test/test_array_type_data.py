import pytest

import pandas as pd
from metacatalog import api
from ._util import connect



# using eddy wind speed data for the tests (u, v, w)
tstamp = "2018-01-01 00:30:00", "2018-01-01 01:00:00", "2018-01-01 01:30:00", "2018-01-01 02:00:00", "2018-01-01 02:30:00", "2018-01-01 03:00:00", "2018-01-01 03:30:00", "2018-01-01 04:00:00", "2018-01-01 04:30:00", "2018-01-01 05:00:00"
u = 1.123902, 0.214753, 0.446611, 0.962977, 2.915902, 4.048897, 5.368552, 6.046246, 5.405221, 4.172279
v = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 , 0.0
w = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 , 0.0

df_3D_wind = pd.DataFrame(data={"tstamp": tstamp, "u_ms": u, "v_ms": v, "w_ms": w})    # use different column names to test force_data_names=True
df_3D_wind['tstamp'] = pd.to_datetime(df_3D_wind['tstamp'], format='%Y-%m-%d %H:%M:%S')
df_3D_wind.set_index('tstamp', inplace=True)



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


def create_3D_datasource(session):
    """
    Add a datasource to the eddy entry.
    """
    entry_3D_wind = api.find_entry(session, title='3-dimensional windspeed data')[0]
    entry_3D_wind.create_datasource(type=1, path='timeseries_array', datatype='timeseries', data_names=['u', 'v', 'w'])

    entry_3D_wind.datasource.create_scale(resolution='30min', extent=(df_3D_wind.index[0], df_3D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    session.commit()

    # assert
    assert entry_3D_wind.datasource.data_names == ['u', 'v', 'w']

    return True


def add_3D_data(session):
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
    assert dat.columns.tolist() == ['u', 'v', 'w']          # at the moment, no precision columns will be returned when there is no data, is this the wanted behaviour?
    assert dat.index[2] == pd.to_datetime("2018-01-01 01:30:00", format='%Y-%m-%d %H:%M:%S')
    assert dat['u'].mean() == 3.070534

    return True


def one_dim_data(session):
    """
    Do the same as above, but with one-dimensional data instead.
    """
    # generate data
    df_1D_wind = pd.DataFrame(data={"tstamp": tstamp, "u_ms": u})
    df_1D_wind['tstamp'] = pd.to_datetime(df_1D_wind['tstamp'], format='%Y-%m-%d %H:%M:%S')
    df_1D_wind.set_index('tstamp', inplace=True)

    # add the variable
    var_1D_wind = api.add_variable(session, name='1D-wind', symbol='u', column_names=['u'], unit=107)

    # find the previously added author
    kit = api.find_person(session, organisation_abbrev='KIT')[0]

    # add the entry
    entry_1D_wind = api.add_entry(session, title='1-dimensional windspeed data',abstract='1-dimensional windspeed data from the Fendt data set',
                                  location=(8, 52),
                                  variable=var_1D_wind.id,
                                  license=6,
                                  author=kit.id,
                                  embargo=False,
                                  is_partial=False)

    # create datasource and scale
    entry_1D_wind.create_datasource(type=1, path='timeseries_array', datatype='timeseries', data_names=['u'])

    entry_1D_wind.datasource.create_scale(resolution='30min', extent=(df_1D_wind.index[0], df_1D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    # add data
    entry_1D_wind.import_data(df_1D_wind)

    # read data
    dat = entry_1D_wind.get_data()

    # assert
    assert dat.columns == 'u'
    assert dat['u'].mean() == 3.070534

    return True


def force_data_names_true(session):
    """
    Test force_data_names=True when loading the data into the database.
    In this case, datasource.data_names will be overwritten with the column
    names of the imported data, when exporting the data, these column col_names
    will be displayed.
    We use the 3D eddy wind data for this again.
    """
    # find the variable
    var_3D_wind = api.find_variable(session, name='3D-wind')[0]

    # find the previously added person
    kit = api.find_person(session, organisation_abbrev='KIT')[0]

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
    entry_3D_force_data_names.create_datasource(type=1, path='timeseries_array', datatype='timeseries', data_names=['u', 'v', 'w'])

    entry_3D_force_data_names.datasource.create_scale(resolution='30min', extent=(df_3D_wind.index[0], df_3D_wind.index[-1]), support=1.0, scale_dimension='temporal')

    # add data
    entry_3D_force_data_names.import_data(df_3D_wind, force_data_names=True)

    #load data
    dat = entry_3D_force_data_names.get_data()

    assert dat.columns.tolist() == ['u_ms', 'v_ms', 'w_ms']
    assert dat['u_ms'].mean() == 3.070534

    return True


# TEST len(data_columns) != len(entry.variable.column_names)

    #### a datasource must always be created first, datasource.data_names is not nullable -> WHEN would we use variable.column_names??


#def test_old_timeseries(session):
#    return True

@pytest.mark.depends(on=['db_init'], name='array_type_data')
def test_array_type_data():
    """
    A simple workflow of 3 persons who contributed to two entries.
    The content of some related content is tested randomly
    """
    # get a session
    session = connect(mode='session')

    # run single tests
    assert add_3D_entry(session)
    assert create_3D_datasource(session)
    assert add_3D_data(session)
    assert read_3D_data(session)
    assert one_dim_data(session)
    assert force_data_names_true(session)
