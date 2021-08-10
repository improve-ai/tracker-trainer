import json
import os 

import boto3


# Launch a reward assignment AWS Batch Job
def lambda_handler(event, context):
    batch = boto3.client('batch')

    worker_count = int(os.environ['WORKER_COUNT'])

    result = batch.submit_job(
        jobName=f"{os.environ['SERVICE']}-{os.environ['STAGE']}-AssignRewards",
        jobQueue=os.environ['JOB_QUEUE'],
        jobDefinition=os.environ['JOB_DEFINITION'],
        containerOverrides={
            'environment': [
                {'name': 'WORKER_COUNT', 'value':
                    str(worker_count)},
                {'name': 'TRAIN_BUCKET', 'value': os.environ['TRAIN_BUCKET']}
            ],
        },
        arrayProperties={
            'size': worker_count
        }
    )

    print(f"submitted batch job {result['jobArn']}")
