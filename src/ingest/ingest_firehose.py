import json
import os 
import boto3

from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import traceback
import threading

from config import TRAIN_BUCKET, FIREHOSE_BUCKET, S3_CONNECTION_COUNT
from firehose_record import FirehoseRecordGroup
from partition import RewardedDecisionPartition


RECORDS_KEY = 'Records'
S3_KEY = 's3'
BUCKET_KEY = 'bucket'
NAME_KEY = 'name'
OBJECT_KEY = 'object'
KEY_KEY = 'key'


# Launch a reward assignment AWS Batch Job
def lambda_handler(event, context):
    
    print(f'processing s3 event {json.dumps(event)}')

    if not RECORDS_KEY in event or len(event[RECORDS_KEY]) != 1:
        raise Exception('Unexpected s3 event format')
        
    record = event[RECORDS_KEY][0]
    
    if not S3_KEY in record or not BUCKET_KEY in record[S3_KEY] or not OBJECT_KEY in record[S3_KEY]:
        raise Exception('Unexpected s3 event format')

    bucket = record[S3_KEY][BUCKET_KEY]
    object_ = record[S3_KEY][OBJECT_KEY]

    if not NAME_KEY in bucket or not KEY_KEY in object_:
        raise Exception('Unexpected s3 event format')

    s3_bucket = bucket[NAME_KEY]
    s3_key = object_[KEY_KEY]
    
    assert(s3_bucket == FIREHOSE_BUCKET)

    # load the incoming firehose file and group records by model name
    firehose_record_groups = FirehoseRecordGroup.load_groups(s3_key)

    decision_partitions = list(map(lambda x: RewardedDecisionPartition(x.model_name, x.to_pandas_df()), firehose_record_groups))
    
    # process each group. consolidate records, upload rewarded decisions to s3
    with ThreadPoolExecutor(max_workers=S3_CONNECTION_COUNT) as executor:
        list(executor.map(lambda x: x.process(), decision_partitions))  # list() forces evaluation of generator
    
    return None

