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

    job_queue_name = os.environ['JOB_QUEUE_NAME']
    job_definition_name = os.environ['JOB_DEFINITION_NAME']
    node_count = os.environ['REWARD_ASSIGNMENT_WORKER_COUNT']
    stage = os.environ['STAGE']
    
    # Submit an AWS Batch job from a job definition.
    # Parameters specified during submitJob override parameters defined in the 
    # job definition.
    r = batch.submit_job(
        jobName=f'improve-v6-assign-rewards-{stage}', 
        # Name or ARN of AWS Batch JobQueue
        jobQueue=job_queue_name, 
        # (name:revision) or ARN of the job definition to deregister
        jobDefinition=job_definition_name,
        # Set some environment variables in Docker
        containerOverrides={
            "environment":[
                {"name": "REWARD_ASSIGNMENT_WORKER_COUNT", "value": node_count},
            ],
        },
        # Size of the collection of jobs to send
        arrayProperties={
            "size": int(node_count)
        }
    )

    print(f"submitted batch job {r['jobName']} arn {r['jobArn']} id {r['jobId']}")
