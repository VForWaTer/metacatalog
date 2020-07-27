import pytest
import subprocess
from subprocess import PIPE
import json

from ._util import connect

# TODO, for now these tests use the data added in the API tests. this 
# should be replace by tests adding the data by CLI

def check_json_output(dburi):
    cmd = ['metacatalog', 'find', 'entry', '--json', '--by', 'author', 'Reeves', '--connection', dburi]
    cmdRes = subprocess.run(cmd, stderr=PIPE, stdout=PIPE)

    results = json.loads(cmdRes.stdout)

    assert len(results) == 2
    return True

@pytest.mark.depends(on=['add_find'], name='cli_find')
def test_cli_find():
    """
    A simple workflow for finding some entries via CLI

    This test has to be changed in the future to also 
    add the data via CLI
    """
    # get a session
    dburi = connect(mode='string')

    # run single tests
    check_json_output(dburi)
