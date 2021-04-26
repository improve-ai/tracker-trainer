import json
import os 

import boto3

RECORDS_KEY = "Records"
S3_KEY = "s3"
BUCKET_KEY = "bucket"
NAME_KEY = "name"
OBJECT_KEY = "object"
KEY_KEY = "key"

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
        
    batch = boto3.client('batch')

    jobQueue = os.environ['JOB_QUEUE']
    jobDefinition = os.environ['JOB_DEFINITION']
    service = os.environ['SERVICE']
    stage = os.environ['STAGE']
    
    r = batch.submit_job(
        jobName=f'{service}-{stage}-ingest-firehose-file', 
        jobQueue=jobQueue, 
        jobDefinition=jobDefinition,
        containerOverrides={
            "environment":[
                {"name": "S3_BUCKET", "value": s3_bucket},
                {"name": "S3_KEY", "value": s3_key}
            ],
        }
    )

    print(f"submitted batch job {r['jobName']} arn {r['jobArn']}")
