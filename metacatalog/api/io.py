"""IO API

The Input/Output API can be used to read and write common 
formats. This is most helpful for adding new records and 
exporting search results.
"""
import os
import io
import shlex
import json
import pandas as pd


def from_csv(file_name_or_content, as_record=True):
    """
    """
    # check filename
    if not os.path.exists(file_name_or_content):
        #file does not exist, thus content is assumed
        fs = io.StringIO()
        fs.write(file_name_or_content)
        fs.seek()
    else:
        # this is already a file path
        fs = file_name_or_content

    # read
    df = pd.read_csv(fs)
    
    if as_record:
        return df.to_dict(orient='record')
    else:
        return df


def from_text(file_name_or_content):
    """
    """
    # get the file content
    if os.path.exists(file_name_or_content):
        with open(file_name_or_content, 'r') as f:
            content = f.read()
    else:
        content = file_name_or_content

    # The key value pairs are separated by whitespace
    # Whitespace in Quotes are not used as separator
    lines = content.split('\n')

    records = []
    for line in lines:
        chunks = shlex.split(line)
        record = dict()

        # build the record
        for chunk in chunks:
            vals = chunk.split('=')
            record[vals[0].strip()] = vals[1].strip()
        records.append(record)

    return records


def from_json(file_name_or_content):
    """
    """
    # check filename
    if not os.path.exists(file_name_or_content):
        # file does not exist, thus content is assumed
        fs = io.StringIO()
        fs.write(file_name_or_content)
        fs.seek()
    else:
        # this is alread a file path
        fs = file_name_or_content

    # read
    records = json.load(fs)

    return records
