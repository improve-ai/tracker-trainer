"""
Configuration file.
Store any constant or configuration variable of interest here.
-------------------------------------------------------------------------------
"""

# Built-in imports
from pathlib import Path
import logging
import os

# The length (in seconds) of the reward window
REWARD_WINDOW = 24 * 2 * 60 * 60

# The worker number of this job
NODE_ID = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])

# The total number of jobs launched
NODE_COUNT = int(os.environ['REWARD_ASSIGNMENT_WORKER_COUNT'])

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

# Logging level
LOGGING_LEVEL = logging.INFO

# Logging format
LOGGING_FORMAT = '%(levelname)-5s: @%(funcName)-25s | %(message)s'

EFS_PATH = Path('/mnt/efs')

INCOMING_FIREHOSE_PATH = EFS_PATH / 'incoming_firehose'

INCOMING_HISTORIES_PATH = EFS_PATH / 'incoming_histories'

HISTORIES_PATH = EFS_PATH / 'histories'

UNRECOVERABLE_PATH = EFS_PATH / 'unrecoverable'

UNRECOVERABLE_INCOMING_FIREHOSE_PATH = UNRECOVERABLE_PATH / 'incoming_firehose'

UNRECOVERABLE_HISTORIES_PATH = UNRECOVERABLE_PATH / 'histories'

DEFAULT_REWARD_KEY = 'reward'

# Starting reward value for a decision record if it doesn't have one
DEFAULT_REWARD_VALUE = 0

# The number of threads in this node's threadpool
THREAD_WORKER_COUNT = 50

# The default reward value of a record of type 'event'
DEFAULT_EVENTS_REWARD_VALUE = 0.001

