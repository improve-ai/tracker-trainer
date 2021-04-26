# Built-in imports
import io
import json
import sys
import hashlib
import gzip
import zlib
import shutil
import uuid
import dateutil
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from itertools import groupby

import config
import worker
import constants

def load_history(file_group):
    records = []
    message_ids = set()
    
    for file in file_group:
        records.extend(load_records(file, message_ids))
        
    return records


def load_records(file, message_ids):
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
                    record[constants.TIMESTAMP_KEY] = dateutil.parser.parse(record[constants.TIMESTAMP_KEY])
                    
                    message_id = record[constants.MESSAGE_ID_KEY]
                    if not message_id in message_ids:
                        message_ids.add(message_id)
                        records.append(record)
                    else:
                        worker.stats.incrementDuplicateMessageIdCount()
                except (json.decoder.JSONDecodeError, ValueError) as e:
                    error = e
    except (zlib.error, EOFError, gzip.BadGzipFile) as e:
        # gzip can throw zlib.error, EOFError, or gzip.BadGZipFile on corrupt file
        error = e
        
    if error:
        # Unrecoverable parse error, copy the file to /unrecoverable
        dest = config.UNRECOVERABLE_PATH / file.name
        print(f'unrecoverable parse error "{error}", copying {file.absolute()} to {dest.absolute()}')
        copy_file(file, dest)
        worker.stats.incrementUnrecoverableFileCount()
    
    return records


def select_incoming_history_files():
    # hash based on the first 8 characters of the hashed history id
    return select_files_for_node(config.INCOMING_PATH, '*.jsonl.gz')
    # TODO check valid file name & hashed history id chars
    
def save_history(hashed_history_id, history_records):
    
    output_file = history_dir_for_hashed_history_id(hashed_history_id) / f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
    
    ensure_parent_dir(output_file)

    save_gzipped_jsonlines(output_file.absolute(), history_records)

def unique_hashed_history_file_name(hashed_history_id):
    return f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
    
def hashed_history_id_from_file(file):
    return file.name.split('-')[0]

def history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return config.HISTORIES_PATH / hashed_history_id[0:2] / hashed_history_id[2:4]

def history_files_for_hashed_history_id(hashed_history_id):
    results = list(history_dir_for_hashed_history_id(hashed_history_id).glob(f'{hashed_history_id}-*.jsonl.gz'))
    return results

def group_files_by_hashed_history_id(files):
    sorted_files = sorted(files, key=hashed_history_id_from_file)
    return [list(it) for k, it in groupby(sorted_files, hashed_history_id_from_file)]    

def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)
        
def copy_file(src, dest):
    ensure_parent_dir(dest)
    shutil.copy(src.absolute(), dest.absolute())

def select_files_for_node(input_dir, glob):

    files_to_process = []
    file_count = 0
    for f in input_dir.glob(glob):
        # TODO bad file names stats
        file_count += 1
        # convert first 8 hex chars (32 bit) to an int
        # the file name starts with a uuidv4
        # check if int mod node_count matches our node
        if (int(f.name[:8], 16) % config.NODE_COUNT) == config.NODE_ID:
            files_to_process.append(f)

    print(f'selected {len(files_to_process)} of {file_count} files from {input_dir}/{glob} to process')
    
    return files_to_process

            
def save_gzipped_jsonlines(file, records):

    with gzip.open(file, mode='w') as gzf:
        for record in records:
            gzf.write((json.dumps(record, default=serialize_datetime) + "\n").encode())
    
    
def upload_gzipped_jsonlines(s3_bucket, s3_key, records):
    
    gzipped = io.BytesIO()
    
    save_gzipped_jsonlines(gzipped, records)

    gzipped.seek(0)
    
    worker.s3client.put_object(Bucket=s3_bucket, Body=gzipped, Key=s3_key)


def upload_rewarded_decisions(model, hashed_history_id, rewarded_decisions):
    # TODO double check model name and hashed_history_id to ensure valid characters
    return upload_gzipped_jsonlines(config.TRAIN_BUCKET, rewarded_decisions_s3_key(model, hashed_history_id), rewarded_decisions)


def delete_all(paths):
    for path in paths:
        path.unlink(missing_ok=True)


def rewarded_decisions_s3_key(model, hashed_history_id):
    return f'rewarded_decisions/{model}/{hashed_history_id[0:2]}/{hashed_history_id[2:4]}/{hashed_history_id}.jsonl.gz'


def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError (f'{type(obj)} not serializable')
    
def hash_history_id(history_id):
    return hashlib.sha256(history_id.encode()).hexdigest()