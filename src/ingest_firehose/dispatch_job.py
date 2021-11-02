import json
import os 


import boto3


RECORDS_KEY = 'Records'
S3_KEY = 's3'
BUCKET_KEY = 'bucket'
NAME_KEY = 'name'
OBJECT_KEY = 'object'
KEY_KEY = 'key'


FIREHOSE_BUCKET = os.getenv('FIREHOSE_BUCKET')
TRAIN_BUCKET = os.getenv('TRAIN_BUCKET')


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
        
    batch = boto3.client('batch')

    r = batch.submit_job(
        jobName=f"{os.environ['SERVICE']}-{os.environ['STAGE']}-IngestFirehose", 
        jobQueue=os.environ['JOB_QUEUE'], 
        jobDefinition=os.environ['JOB_DEFINITION'],
        containerOverrides={
            'environment':[
                {'name': 'TRAIN_BUCKET', 'value': TRAIN_BUCKET},
                {'name': 'FIREHOSE_BUCKET', 'value': FIREHOSE_BUCKET},
                {'name': 'S3_KEY', 'value': s3_key}
            ],
        }
    )

    print(f"submitted batch job {r['jobArn']}")
