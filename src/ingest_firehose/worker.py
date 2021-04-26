import os
import signal
import boto3
import gzip
import json
import hashlib
import uuid
from pathlib import Path

s3 = boto3.resource("s3")

S3_BUCKET = os.environ['S3_BUCKET']
S3_KEY = os.environ['S3_KEY']

INCOMING_PATH = Path('/mnt/efs/incoming')

# execute the core worker loop of processing incoming firehose files then 
# processing incoming history files
def worker():
    print(f"starting firehose ingest for bucket {S3_BUCKET} key {S3_KEY}")

    records_by_history_id = {}
    
    bad_history_id_count = 0

    # download and parse the firehose file
    obj = s3.Object(S3_BUCKET, S3_KEY)
    with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzf:
        for line in gzf.readlines():
            record = json.loads(line)
            
            history_id = record.get('history_id', None)
            if not history_id or not isinstance(history_id, str) or not len(history_id):
                bad_history_id_count += 1
                continue
                
            if not history_id in records_by_history_id:
                records_by_history_id[history_id] = []
                
            records_by_history_id[history_id].append(record)
            
    if bad_history_id_count:
        print(f'skipped {bad_history_id_count} records with bad history_id fields')

    file_count = 0
    record_count = 0

    # save each set of records to an incoming history file
    for history_id, records in records_by_history_id.items():
        hashed_history_id = hashlib.sha256(history_id.encode()).hexdigest()
        file = INCOMING_PATH / f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
        
        ensure_parent_dir(file)
        
        with gzip.open(file, mode='w') as gzf:
            for record in records:
                gzf.write((json.dumps(record) + "\n").encode())

        file_count += 1
        record_count += len(records)
        
    print(f'wrote {record_count} records to {file_count} files incoming history files')
    print('finished firehose ingest job')

def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)


def signal_handler(signalNumber, frame):
    # don't actually handle the signal.  Ingest should take less than the 2 minute
    # spot instance shutdown period and we can't checkpoint it anyway.
    print(f"SIGTERM received")
    return


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
