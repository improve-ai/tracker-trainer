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

# The length (timedelta) of the reward window
_reward_window_in_seconds = int(os.environ['REWARD_WINDOW_IN_SECONDS'])
REWARD_WINDOW = timedelta(seconds=_reward_window_in_seconds)

# The worker number of this job
NODE_ID = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])

# The length of the batch job array
NODE_COUNT = int(os.environ['WORKER_COUNT'])

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

EFS_PATH = Path('/mnt/efs')

INCOMING_PATH = EFS_PATH / 'incoming'

HISTORIES_PATH = EFS_PATH / 'histories'

UNRECOVERABLE_PATH = EFS_PATH / 'unrecoverable'

# The default reward value of a record of type 'event'
DEFAULT_EVENT_VALUE = float(os.environ.get('DEFAULT_EVENT_VALUE', 0.0))

# One time bonus reward per decision for any activity (any record) after the reward window
RETENTION_BONUS = float(os.environ.get('RETENTION_BONUS', 0.0))
