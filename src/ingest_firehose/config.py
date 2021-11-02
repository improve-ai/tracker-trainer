from pathlib import Path
import boto3
import botocore
import os
from datetime import timedelta

from stats import Stats

stats = Stats()

# The number of threads in this node's threadpool. Must have enough memory for each thread to load a full history
THREAD_WORKER_COUNT = 16

# boto3 client must be pre-initialized for multi-threaded (https://github.com/boto/botocore/issues/1246)
s3client = boto3.client("s3", config=botocore.config.Config(max_pool_connections=THREAD_WORKER_COUNT))

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

FIREHOSE_BUCKET = os.environ['FIREHOSE_BUCKET']

# the incoming firehose file to ingest
INCOMING_FIREHOSE_S3_KEY = os.environ['S3_KEY']
