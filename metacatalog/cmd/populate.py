import os

import pandas as pd

from ._util import connect
from metacatalog import DATAPATH
from metacatalog import DataSourceType



def import_table_data(fname, InstanceClass):
    df = pd.read_csv(os.path.join(DATAPATH, fname))

    # build an instance for each line and return
    return [InstanceClass(**d) for d in df.to_dict(orient='record')]
    

def populate(args):
    # get a session to the database
    session = connect(args)

    # now, populate

    # data source types
    datatypes = import_table_data('datasource_types.csv', DataSourceType)

    try:
        print('Populating data source types')
        session.add_all(datatypes)
        session.commit()
    except Exception as e:
        print('Failed.\n%s' % str(e))
        session.rollback()

    print('Done.')