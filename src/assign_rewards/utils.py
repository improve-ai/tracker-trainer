# Built-in imports
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import logging
import json
import sys
import hashlib
import gzip
import zlib
from config import UNRECOVERABLE_PATH
import shutil

# The timestamp format of the records
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
HISTORY_ID_KEY = 'history_id'
TYPE_KEY = 'type'
DECISION_KEY = 'decision'
MODEL_KEY = 'model'
TIMESTAMP_KEY = 'timestamp'

def load_history(hashed_history_id, file_group):
    records = []

    for file in file_group:
        try:
            records.extend(load_records(hashed_history_id, file))
        except (json.decoder.JSONDecodeError, zlib.error, EOFError) as e: #TODO add gzip.BadGzipFile for Python 3.8
            # Unrecoverable parse error, move file to /unrecoverable
            print(f'unrecoverable error {e} parsing {file.absolute()}, moving file to EFS /unrecoverable')
            move_to_unrecoverable(file)
            
    return records

def move_to_unrecoverable(file):
    dest = UNRECOVERABLE_PATH / file.name
    ensure_parent_dir(dest)
    # Don't use Pathlib.rename because the original Path object may be deleted
    shutil.move(file.absolute(), dest.absolute())
    
def ensure_parent_dir(file):
    parent_dir = file.parent()
    if not parent_dir.exists():
        print(f'creating {str(parent_dir)}')
        parent_dir.mkdir(parents=True, exist_ok=True)

def load_records(hashed_history_id, file):
    """
    Load a jsonlines file
    
    Args:
        filename: name of the input jsonlines file to load
    
    Returns:
        A list of records, and count of errors
    """

    records = []

    with gzip.open(file.absolute(), encoding="utf8") as gzf:
        for line in gzf.readlines():
            records.append(json.loads(line))

    return records

def validate_records():
    history_id = None

    # parse the timestamp into a datetime since it will be used often
    # throws ValueError on invalid timestamp
    record[TIMESTAMP_KEY] = datetime.strptime(record[TIMESTAMP_KEY], DATETIME_FORMAT)

    validate_record(record, history_id, hashed_history_id)
    
    if not history_id:
        # history_id has been validated to hash to the hashed_history_id
        history_id = record[HISTORY_ID_KEY]


# TODO be careful when allowing developers to override validation, because if they have a bug it will currently delete records during the consolidation write
# (or if we have a bug in this library)
def validate_record(record, history_id, hashed_history_id):
    if not TIMESTAMP_KEY in record or not TYPE_KEY in record or not HISTORY_ID_KEY in record:
        raise ValueError('invalid record')
    
    # 'decision' type requires a 'model'
    if record[TYPE_KEY] == DECISION_KEY and not MODEL_KEY in record:
        raise ValueError('invalid record')
        
    # TODO check valid model name characters
    
    if history_id:
        # history id provided, see if they match
        if history_id != record[HISTORY_ID_KEY]:
            raise ValueError('history_id hash mismatch')
    elif not hashlib.sha256(record[HISTORY_ID_KEY].encode()).hexdigest() == hashed_history_id:
        raise ValueError('history_id hash mismatch')

# TODO REMOVE?
def name_no_ext(p):
    """Given a Path object or a str, return a filename without extensions"""
    if isinstance(p, Path):
        return p.stem.split('.')[0]
    if isinstance(p, str):
        return p.split(".")[0]

# TODO REMOVE?
def deepcopy(o):
    return json.loads(json.dumps(o))