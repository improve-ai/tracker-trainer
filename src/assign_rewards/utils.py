# Built-in imports
from datetime import datetime
from datetime import timedelta
import dateutil
from pathlib import Path
import logging
import json
import sys
import hashlib
import gzip
import zlib
from config import UNRECOVERABLE_PATH
import shutil

import config

# stats keys
DUPLICATE_MESSAGE_ID_COUNT = "Duplicate Records"
RECORD_COUNT = "Unique Records"
UNRECOVERABLE_PARSE_ERROR_COUNT = "Unrecoverable Record Parse Errors"

HISTORY_ID_KEY = 'history_id'
TYPE_KEY = 'type'
DECISION_KEY = 'decision'
MODEL_KEY = 'model'
TIMESTAMP_KEY = 'timestamp'
MESSAGE_ID_KEY = 'message_id'

def load_history(file_group, stats):
    records = []
    message_ids = set()
    
    for file in file_group:
        records.extend(load_records(file, message_ids, stats))
            
    return records

def copy_to_unrecoverable(file):
    dest = UNRECOVERABLE_PATH / file.name
    ensure_parent_dir(dest)
    shutil.copy(file.absolute(), dest.absolute())
    
def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)

def load_records(file, message_ids, stats):
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
                    record[TIMESTAMP_KEY] = dateutil.parser.parse(record[TIMESTAMP_KEY])
                    
                    message_id = record[MESSAGE_ID_KEY]
                    if not message_id in message_ids:
                        message_ids.add(message_id)
                        records.append(record)
                    else:
                        stats[DUPLICATE_MESSAGE_ID_COUNT] += 1
                except (json.decoder.JSONDecodeError, ValueError) as e:
                    error = e
    except (zlib.error, EOFError, gzip.BadGzipFile) as e:
        # gzip can throw zlib.error, EOFError, or gzip.BadGZipFile on corrupt file
        error = e
        
    if error:
        # Unrecoverable parse error, copy file to /unrecoverable
        print(f'unrecoverable parse error "{error}", copying {file.absolute()} to {UNRECOVERABLE_PATH.absolute()}')
        stats[UNRECOVERABLE_PARSE_ERROR_COUNT] += 1
        copy_to_unrecoverable(file)
    
    
    stats[RECORD_COUNT] += len(records)

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
    elif not hash_history_id(record[HISTORY_ID_KEY]) == hashed_history_id:
        raise ValueError('history_id hash mismatch')

def select_incoming_history_files(stats):
    # hash based on the first 8 characters of the hashed history id
    return select_files_for_node(INCOMING_HISTORIES_PATH, '*.jsonl.gz', stats)
    # TODO check valid file name & hashed history id chars


def select_files_for_node(input_dir, glob, stats):

    files_to_process = []
    file_count = 0
    for f in input_dir.glob(glob):
        # TODO bad file names stats
        file_count += 1
        # convert first 8 hex chars (32 bit) to an int
        # the file name starts with a uuidv4
        # check if int mod node_count matches our node
        if (int(f.name[:8], 16) % REWARD_ASSIGNMENT_WORKER_COUNT) == AWS_BATCH_JOB_ARRAY_INDEX:
            files_to_process.append(f)

    print(f'selected {len(files_to_process)} of {file_count} files from {input_dir}/*.jsonl.gz to process')
    
    # TODO fix these stats
    stats[TOTAL_INCOMING_FILE_COUNT] += file_count
    stats[PROCESSED_INCOMING_FILE_COUNT] += len(files_to_process)
    
    return files_to_process

def save_history(hashed_history_id, history_records):
    
    output_file = history_dir_for_hashed_history_id(hashed_history_id) / f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
    save_gzipped_jsonlines(output_file, history_records)

def unique_hashed_history_file_name(hashed_history_id):
    return f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
            
def save_gzipped_jsonlines(file, records):
    ensure_parent_dir(file)

    with gzip.open(file.absolute(), mode='w') as gzf:
        for record in records:
            gzf.write((json.dumps(record, default=serialize_datetime) + "\n").encode())
    

def upload_rewarded_decisions(model, hashed_history_id, rewarded_decisions):
    # TODO double check model name and hashed_history_id to ensure valid characters
    
    gzipped = io.BytesIO()
    
    with gzip.open(gzipped, mode='w') as gzf:
        for record in rewarded_decisions:
            gzf.write((json.dumps(record, default=serialize_datetime) + "\n").encode())
    
    gzipped.seek(0)
    
    s3_key = rewarded_decisions_s3_key(model, hashed_history_id)
    s3client.put_object(Bucket=TRAIN_BUCKET, Body=gzipped, Key=s3_key)

def delete_all(paths):
    for path in paths:
        path.unlink(missing_ok=True)



def rewarded_decisions_s3_key(model, hashed_history_id):
    return f'rewarded_decisions/{model}/{hashed_history_id[0:2]}/{hashed_history_id[2:4]}/{hashed_history_id}.jsonl.gz'

def hashed_history_id_from_file(file):
    return file.name.split('-')[0]

def history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return config.HISTORIES_PATH / sub_dir_for_hashed_history_id(hashed_history_id)

def incoming_history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/incoming_histories/1c/aa
    return config.INCOMING_HISTORIES_PATH / sub_dir_for_hashed_history_id(hashed_history_id)

def sub_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return hashed_history_id[0:2] / hashed_history_id[2:4]

def history_files_for_hashed_history_id(hashed_history_id, stats):
    results = list(history_dir_for_hashed_history_id(hashed_history_id).glob(f'{hashed_history_id}-*.jsonl.gz'))
    stats[PROCESSED_HISTORY_FILE_COUNT] += len(results)
    return results

def group_files_by_hashed_history_id(files):
    sorted_files = sorted(files, key=hashed_history_id_from_file)
    return [list(it) for k, it in groupby(sorted_files, hashed_history_id_from_file)]    

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError (f'{type(obj)} not serializable')
    
def create_stats():
    return defaultdict(int) # calls int() to set the default value of 0 on missing key
    
def hash_history_id(history_id):
    return hashlib.sha256(history_id.encode()).hexdigest()