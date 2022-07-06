import pytest
import pandas as pd
import numpy as np
from datetime import datetime as dt

from metacatalog import api
from metacatalog.util.results import ResultList
from ._util import connect, read_to_df


ENTRIES = """title,author,x,y,variable,abstract,license
"Random Set: Old Record",Simpson,7.8,48.2,5,"Lorem ipsum",5
"Random Set: Recent Record",Simpson,7.8,48.2,5,"Lorem ipsum",5
"Random Set: Auxiliary data",Simpson,7.8,48.2,5,"Lorem ipsum",5
"""


def add_entries(session):
    df = read_to_df(ENTRIES)
    df['location'] = [(t[0], t[1],) for t in df[['x', 'y']].values]
    df = df.where(df.notnull(), None)
    df.drop(['x', 'y'], axis=1, inplace=True)
    entries_dict_list = df.to_dict(orient='records')

    # add
    entries = [api.add_entry(session, **e) for e in entries_dict_list]
    assert len(entries) == 3

    # add group
    composite = entries[0].make_composite(
        others=entries[1:],
        title='Random Set',
        description='Set of random data',
        commit=True
    )

    # split dataset
    split_entries = api.find_entry(session, title='Random Set: * Record')
    assert len(split_entries) == 2
    split_group = api.add_group(
        session,
        'Split dataset',
        [e.id for e in split_entries],
        title='Random Set: Full Record',
        description='Combination of Old and Recent Record'
    )
    assert len(split_group.entries) == 2

    return True


def add_data(session):
    # find the entries
    entries = api.find_entry(session, title='Random Set:*')
    assert len(entries) == 3

    # get each entry
    old = [e for e in entries if 'Old' in e.title][0]
    new = [e for e in entries if 'Recent' in e.title][0]
    aux = [e for e in entries if 'Auxiliary' in e.title][0]

    # create the random data
    idx = pd.date_range('201806131100', freq='15min', periods=96)
    nidx = pd.date_range('201806141100', freq='5min', periods=220)
    df = pd.DataFrame({'tstamp': idx, 'data': np.random.normal(15, 3, size=96)})
    df.set_index('tstamp', inplace=True)
    ndf = pd.DataFrame({'tstamp': nidx, 'data': np.random.normal(15, 3, size=220)})
    ndf.set_index('tstamp', inplace=True)
    aux_df = pd.DataFrame({'tstamp': idx, 'data': np.random.gamma(35, 5, size=96)})
    aux_df.set_index('tstamp', inplace=True)
    
    # upload
    aux.create_datasource(type=1, path='timeseries', datatype='timeseries', commit=True)
    aux.datasource.create_scale(resolution='15min', extent=[aux_df.index.min(), aux_df.index.max()], support=1.0, scale_dimension='temporal')
    aux.import_data(data=aux_df)

    old.create_datasource(type=1, path='timeseries', datatype='timeseries', commit=True)
    old.datasource.create_scale(resolution='15min', extent=[df.index.min(), df.index.max()], support=0.5, scale_dimension='temporal')
    old.import_data(data=df)

    new.create_datasource(type=1, path='timeseries', datatype='timeseries', commit=True)
    new.datasource.create_scale(resolution='5min', extent=[ndf.index.min(), ndf.index.max()], support=1.0, scale_dimension='temporal')
    new.import_data(data=ndf)

    return True


def result_list_check_temporal_scale(session):
    # find the entries
    entries = api.find_entry(session, title='Random Set:*')

    # instantiate the ResultList directly
    rl = ResultList(*entries)

    # this should only contain the composite
    assert len(rl) == 1

    # get the scale triplet
    triplet = rl.temporal_scale
    
    assert triplet.get('support') - 0.5 < 0.001
    assert triplet.get('resolution') == 'P0DT0H15M0S'
    assert triplet.get('extent', [None])[0] == dt(2018, 6, 13, 11, 0)
    assert triplet.get('extent', [None, None])[1] == dt(2018, 6, 15, 5, 15)

    return True


def result_list_check_index(session):
    # get all entries
    entries = api.find_entry(session)
    resultSet = api.find_entry(session, title='Random Set: Recent Record', as_result=True)[0]

    rl = ResultList(*entries)

    # get the index of the object
    idx = rl.index('Random set')
    idx_obj = rl.index(resultSet)

    # should be the same
    assert idx == idx_obj
    
    return True


def result_list_check_append(session):
    # get Hughes entries
    entries = api.find_entry(session, author='Hughes')

    rl = ResultList(*entries)
    oldLen = len(rl)
    
    # append the Random Set dataset
    randomSet = api.find_entry(session, title='Random Set:*')[0]
    rl.append(randomSet)

    newLen = len(rl)

    assert oldLen + 1 == newLen
    
    return True

@pytest.mark.depends(on=['add_find'], name='resutlset')
def test_result_set():
    """
    ImmutableResultSet and ResultList tests to check that 
    result list merging is working correctly.
    """
    # create a session
    session = connect(mode='session')

    # run single tests
    assert add_entries(session)
    assert add_data(session)
    assert result_list_check_temporal_scale(session)
