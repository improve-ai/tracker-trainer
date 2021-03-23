"""
Launch AWS Batch jobs (from a Lambda function).
-------------------------------------------------------------------------------
"""

# Built-in imports
import json
import os 

# External imports
import boto3


def lambda_handler(event, context):
    """
    Submit an array job.
    """
    batch = boto3.client('batch')

    jobQueue = os.environ['JOB_QUEUE']
    jobDefinition = os.environ['JOB_DEFINITION']
    nodeCount = os.environ['REWARD_ASSIGNMENT_WORKER_COUNT']
    service = os.environ['SERVICE']
    stage = os.environ['STAGE']
    
    # Submit an AWS Batch job from a job definition.
    # Parameters specified during submitJob override parameters defined in the 
    # job definition.
    r = batch.submit_job(
        jobName=f'{service}-{stage}-assign-rewards', 
        # Name or ARN of AWS Batch JobQueue
        jobQueue=jobQueue, 
        # (name:revision) or ARN of the job definition to deregister
        jobDefinition=jobDefinition,
        # Set some environment variables in Docker
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

    print(f"submitted batch job {r['jobName']} arn {r['jobArn']}")
