import json

from concurrent.futures import ThreadPoolExecutor

from config import FIREHOSE_BUCKET, S3_CONNECTION_COUNT
from firehose_record import FirehoseRecordGroup
from partition import RewardedDecisionPartition


RECORDS_KEY = 'Records'
S3_KEY = 's3'
BUCKET_KEY = 'bucket'
NAME_KEY = 'name'
OBJECT_KEY = 'object'
KEY_KEY = 'key'


def lambda_handler(event, context):
    """
    Ingests a gzipped JSONlines file from the firehose S3 bucket and ouputs one merged rewarded decision partition file per model to the train bucket.
    
    In order to keep the ingest process fast, simple, and performing as few S3 operations as possible, only a single partition is written
    per model unless the partition contains over 10,000 records. By default, the firehose process creates a new file every 15 minutes, 
    so groups of decisions and their rewards that occur during that window will be merged together in the resulting partition. 
    For decisions occuring outside of that 15 minute window or late arriving records, the groom process, which is triggered before training, 
    will check to see if this partition overlaps with other partitions and will merge the partitions  together ensuring that any partial rewarded 
    decision records (records that just contain rewards) are merged with their decisions.
    
    Merged partitions containing over 10,000 records will be split into multiple partitions using the logic defined in 
    partition.maybe_split_on_timestamp_boundaries(). 
    """
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

