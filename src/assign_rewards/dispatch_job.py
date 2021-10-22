import json
import os 

import boto3


assign_rewards_envvars = [
    'REWARD_WINDOW_IN_SECONDS',
    'WORKER_COUNT',
    'TRAIN_BUCKET',
    'DEFAULT_EVENT_VALUE']


# Launch a reward assignment AWS Batch Job
def lambda_handler(event, context):
    batch = boto3.client('batch')

    result = batch.submit_job(
        jobName=f"{os.environ['SERVICE']}-{os.environ['STAGE']}-AssignRewards",
        jobQueue=os.environ['JOB_QUEUE'],
        jobDefinition=os.environ['JOB_DEFINITION'],
        containerOverrides={
            'environment': [{'name': key, 'value': os.getenv(key)} for key in assign_rewards_envvars],
        },
        arrayProperties={
            'size': int(os.environ['WORKER_COUNT'])
        }
    )

    print(f"submitted batch job {result['jobArn']}")
