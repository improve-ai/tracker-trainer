from pathlib import Path
import boto3
import botocore
import os
import stats

stats = stats.Stats()

# The number of threads in this node's threadpool. Must have enough memory for each thread to load a full history
THREAD_WORKER_COUNT = 20

# boto3 client must be pre-initialized for multi-threaded (https://github.com/boto/botocore/issues/1246)
s3client = boto3.client("s3", config=botocore.config.Config(max_pool_connections=THREAD_WORKER_COUNT))

# The length (in seconds) of the reward window
REWARD_WINDOW = 24 * 2 * 60 * 60

# The worker number of this job
NODE_ID = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])

# The total number of jobs launched
NODE_COUNT = int(os.environ['REWARD_ASSIGNMENT_WORKER_COUNT'])

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

EFS_PATH = Path('/mnt/efs')

INCOMING_PATH = EFS_PATH / 'incoming'

HISTORIES_PATH = EFS_PATH / 'histories'

UNRECOVERABLE_PATH = EFS_PATH / 'unrecoverable'

# The default reward value of a record of type 'event'
DEFAULT_EVENT_REWARD_VALUE = 0.001

# The timestamp format of the records
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
