import json
import os 

import boto3


# Launch a reward assignment AWS Batch Job
def lambda_handler(event, context):
    batch = boto3.client('batch')

    result = batch.submit_job(
        jobName=f"{os.environ['SERVICE']}-{os.environ['STAGE']}-AssignRewards",
        jobQueue=os.environ['JOB_QUEUE'],
        jobDefinition=os.environ['JOB_DEFINITION'],
        containerOverrides={
            'environment': [{'name': key, 'value': value} for key, value in os.environ.items()],
        },
        arrayProperties={
            'size': int(os.environ['WORKER_COUNT'])
        }
    )

    print(f"submitted batch job {result['jobArn']}")
