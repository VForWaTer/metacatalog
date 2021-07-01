import pytest
import pandas as pd
import numpy as np

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
    idx = pd.date_range('201806131100', freq='15min', periods=100)
    nidx = pd.date_range('201806140900', freq='5min', periods=220)
    df = pd.DataFrame({'tstamp': idx, 'data': np.random.normal(15, 3, size=100)})
    ndf = pd.DataFrame({'tstamp': nidx, 'data': np.random.normal(15, 3, size=220)})
    aux_df = pd.DataFrame({'tstamp': idx, 'data': np.random.gamma(35, 5, size=100)})
    
    # upload
    aux.create_datasource(type=1, path='timeseries', datatype='timeseries')
    aux.datasource.create_scale(resolution='15min', extent=[aux_df.tstamp.min(), aux_df.tstamp.max()], support=1.0, scale_dimension='temporal')
    aux.import_data(data=aux_df)

    old.create_datasource(type=1, path='timeseries', datatype='timeseries')
    old.datasource.create_scale(resolution='15min', extent=[df.tstamp.min(), df.tstamp.max()], support=0.5, scale_dimension='temporal')
    old.import_data(data=df)

    new.create_datasource(type=1, path='timeseries', datatype='timeseries')
    new.datasource.create_scale(resolution='5min', extent=[ndf.tstamp.min(), ndf.tstamp.max()], support=1.0, scale_dimension='temporal')
    old.import_data(data=ndf)

    return True


def result_list_checks(session):
    # find the entries
    entries = api.find_entry(session, title='Random Set:*')

    # instantiate the ResultList directly
    rl = ResultList(*entries)

    # this should only contain the composite
    assert len(rl) == 1

    triplet = rl.temporal_scale
    print(triplet)

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
    assert result_list_checks(session)
