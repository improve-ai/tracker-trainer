import boto3
import botocore
import os

from stats import Stats

DEBUG = False
stats = Stats()

# The number of threads in this node's threadpool. Instance must have enough memory for each thread to load a full .parquet
# plus the memory required to load the full firehose file.
THREAD_WORKER_COUNT = 16

PARQUET_FILE_MAX_DECISION_RECORDS = 10000

# boto3 client must be pre-initialized for multi-threaded (https://github.com/boto/botocore/issues/1246)
s3client = boto3.client("s3", config=botocore.config.Config(max_pool_connections=THREAD_WORKER_COUNT))

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

FIREHOSE_BUCKET = os.environ['FIREHOSE_BUCKET']
