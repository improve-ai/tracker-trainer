"""
Launch AWS Batch jobs (from a Lambda function).
-------------------------------------------------------------------------------
"""

# Built-in imports
import json
import logging
import os 

# External imports
import boto3


def lambda_handler(event, context):
    """
    Submit an array job
    """
    batch = boto3.client('batch')

    job_queue_name = os.environ['JOB_QUEUE_NAME']
    job_definition_name = os.environ['JOB_DEFINITION_NAME']
    node_count = os.environ['JOIN_REWARDS_JOB_ARRAY_SIZE']
    reward_window_seconds = os.environ['DEFAULT_REWARD_WINDOW_IN_SECONDS']
    
    # Submit an AWS Batch job from a job definition.
    # Parameters specified during submitJob override parameters defined in the 
    # job definition.
    r = batch.submit_job(
        jobName='batch-processing-job', 
        # Name or ARN of AWS Batch JobQueue
        jobQueue=job_queue_name, 
        # (name:revision) or ARN of the job definition to deregister
        jobDefinition=job_definition_name,
        # Override the Docker CMD
        containerOverrides={
            "environment":[
                {"name": "JOIN_REWARDS_JOB_ARRAY_SIZE", "value": node_count},
                {"name": "DEFAULT_REWARD_WINDOW_IN_SECONDS", "value": reward_window_seconds},
                {"name": "JOIN_REWARDS_REPROCESS_ALL", "value": False}
            ]
        },
        # Size of the collection of jobs to send
        arrayProperties={
            "size": node_count
        }
    )

    logging.info(
        f"Started a new AWS Batch job,"
        f"Job Name: '{r['jobName']}'"
        f"Job ARN: '{r['jobArn']}'"
        f"Job Id: '{r['jobId']}'"
    )