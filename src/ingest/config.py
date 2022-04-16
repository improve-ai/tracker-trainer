import boto3
import botocore
import os

from stats import Stats


S3_CONNECTION_COUNT = 16

stats = Stats()

# boto3 client must be pre-initialized for multi-threaded (https://github.com/boto/botocore/issues/1246)
s3client = boto3.client("s3", config=botocore.config.Config(max_pool_connections=S3_CONNECTION_COUNT))

PARQUET_FILE_MAX_DECISION_RECORDS = 10000

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

FIREHOSE_BUCKET = os.environ['FIREHOSE_BUCKET']
