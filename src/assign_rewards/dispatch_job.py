import json
import os 

import boto3

# Launch a reward assignment AWS Batch Job
def lambda_handler(event, context):
    batch = boto3.client('batch')

    jobQueue = os.environ['JOB_QUEUE']
    jobDefinition = os.environ['JOB_DEFINITION']
    nodeCount = os.environ['REWARD_ASSIGNMENT_WORKER_COUNT']
    service = os.environ['SERVICE']
    stage = os.environ['STAGE']
    
    r = batch.submit_job(
        jobName=f'{service}-{stage}-assign-rewards', 
        jobQueue=jobQueue, 
        jobDefinition=jobDefinition,
        containerOverrides={
            "environment":[
                {"name": "REWARD_ASSIGNMENT_WORKER_COUNT", "value": nodeCount},
                {"name": "TRAIN_BUCKET", "value": os.environ['TRAIN_BUCKET']}
            ],
        },
        # Size of the collection of jobs to send
        arrayProperties={
            "size": int(nodeCount)
        }
    )

    print(f"submitted batch job {r['jobArn']}")
