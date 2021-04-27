import json
import os 

import boto3

# Launch a reward assignment AWS Batch Job
def lambda_handler(event, context):
    batch = boto3.client('batch')

    nodeCount = int(os.environ['REWARD_ASSIGNMENT_WORKER_COUNT'])

    result = batch.submit_job(
        jobName=f"{os.environ['SERVICE']}-{os.environ['STAGE']}-assign-rewards", 
        jobQueue=os.environ['JOB_QUEUE'], 
        jobDefinition=os.environ['JOB_DEFINITION'],
        containerOverrides={
            "environment":[
                {"name": "REWARD_ASSIGNMENT_WORKER_COUNT", "value": str(nodeCount)},
                {"name": "TRAIN_BUCKET", "value": os.environ['TRAIN_BUCKET']}
            ],
        },
        # Size of the collection of jobs to send
        arrayProperties={
            "size": nodeCount
        }
    )

    print(f"submitted batch job {result['jobArn']}")
