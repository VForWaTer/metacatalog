import pytest

import pandas as pd
from metacatalog import api



### EDDY data chunk erstellen
# using 3D eddy wind speed data (u, v, w)
tstamp = "2018-01-01 00:30:00", "2018-01-01 01:00:00", "2018-01-01 01:30:00", "2018-01-01 02:00:00", "2018-01-01 02:30:00", "2018-01-01 03:00:00", "2018-01-01 03:30:00", "2018-01-01 04:00:00", "2018-01-01 04:30:00", "2018-01-01 05:00:00"
u = 1.123902, 0.214753, 0.446611, 0.962977, 2.915902, 4.048897, 5.368552, 6.046246, 5.405221, 4.172279
v = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 , 0.0
w = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 , 0.0

df = pd.DataFrame(data={"tstamp": tstamp, "u": u, "v": v, "w": w})
df['tstamp'] = pd.to_datetime(df['tstamp'], format='%Y-%m-%d %H:%M:%S')
df.set_index('tstamp', inplace=True)


def add_eddy_entry(session):
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
    eddy_wind = api.add_entry(session, title='3-dimensional windspeed data',
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


def create_eddy_datasource(session):
    """
    Add a datasource to the eddy entry.
    """
    wind.create_datasource(type=1, path='timeseries_array', datatype='timeseries_array', data_names=['u', 'v', 'w'])

    wind.datasource.create_scale(
        resolution='30min',
        extent=(df.index[0], df.index[-1]),
        support=1.0,
        scale_dimension='temporal'
    )

    session.commit()

    # assert
    assert wind.datasource.data_names == ['u', 'v', 'w']

    return True


def add_eddy_data(session):
    """
    Add the previously generated 3D windspeed data to the eddy entry.
    """
    wind.import_data(df)

    return True


def read_eddy_data(session):
    """
    Read the 3D windspeed data and check column names.
    """
    eddy = api.find_entry(session, title='3-dimensional windspeed data')

    dat = eddy.get_data()

    print(dat.columns)

    return True




@pytest.mark.depends(on=['db_init'], name='array_type_data')
def test_array_type_data():
    """
    A simple workflow of 3 persons who contributed to two entries.
    The content of some related content is tested randomly
    """
    # get a session
    session = connect(mode='session')

    # run single tests
    assert add_eddy_entry(session)
    assert create_eddy_datasource(session)
    assert read_eddy_data(session)
