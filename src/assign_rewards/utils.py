# Built-in imports
from datetime import datetime
from datetime import timedelta
import dateutil
from pathlib import Path
import io
import json
import sys
import hashlib
import gzip
import zlib
import shutil

import config
import worker

HISTORY_ID_KEY = 'history_id'
TYPE_KEY = 'type'
DECISION_KEY = 'decision'
MODEL_KEY = 'model'
TIMESTAMP_KEY = 'timestamp'
MESSAGE_ID_KEY = 'message_id'


def copy_to_unrecoverable(file):
    dest = config.UNRECOVERABLE_PATH / file.name
    ensure_parent_dir(dest)
    shutil.copy(file.absolute(), dest.absolute())
    
def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)

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

    ensure_parent_dir(file)

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