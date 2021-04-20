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

def load_history(file_group):
    records = []

    for file in file_group:
        records.extend(load_records(file))
            
    return records

def copy_to_unrecoverable(file):
    dest = UNRECOVERABLE_PATH / file.name
    ensure_parent_dir(dest)
    shutil.copy(file.absolute(), dest.absolute())
    
def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        print(f'creating {str(parent_dir)}')
        parent_dir.mkdir(parents=True, exist_ok=True)

def load_records(file):
    """
    Load a gzipped jsonlines file
    
    Args:
        filename: name of the input gzipped jsonlines file to load
    
    Returns:
        A list of records
    """

    records = []
    error = None

    try:
        with gzip.open(file.absolute(), mode="rt", encoding="utf8") as gzf:
            for line in gzf.readlines():
                # Do a inner try/except to try to recover as many records as possible
                try: 
                    record = json.loads(line)
                    # parse the timestamp into a datetime since it will be used often
                    record[TIMESTAMP_KEY] = datetime.strptime(record[TIMESTAMP_KEY], DATETIME_FORMAT)
                    records.append(record)
                except (json.decoder.JSONDecodeError, ValueError) as e:
                    error = e
    except (zlib.error, EOFError) as e: #TODO add gzip.BadGzipFile for Python 3.8
        # gzip can throw zlib.error, EOFError, or gzip.BadGZipFile on corrupt file
        error = e
        
    if error:
        # Unrecoverable parse error, copy file to /unrecoverable
        print(f'unrecoverable parse error {e}, copying {file.absolute()} to {UNRECOVERABLE_PATH.absolute()}')
        copy_to_unrecoverable(file)

    return records


def filter_valid_records(hashed_history_id, records):

    results = []
    
    history_id = None
    skipped_record_count = 0

    for record in records:
        try:
            validate_record(record, history_id, hashed_history_id)

            if not history_id:
                # history_id has been validated to hash to the hashed_history_id
                history_id = record[HISTORY_ID_KEY]

            results.append(record)
        except Exception as e:
            skipped_record_count += 1
        
    if skipped_record_count:
        print(f'skipped {skipped_record_count} invalid records for hashed history_id {hashed_history_id}')
        
    return results 

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