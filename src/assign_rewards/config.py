"""
Configuration file.
Store any constant or configuration variable of interest here.
-------------------------------------------------------------------------------
"""

# Built-in imports
from pathlib import Path
import os

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

DEFAULT_REWARD_KEY = 'reward'

# Starting reward value for a decision record if it doesn't have one
DEFAULT_REWARD_VALUE = 0

# The number of threads in this node's threadpool. Must have enough memory for each thread to load a full history
THREAD_WORKER_COUNT = 20

# The default reward value of a record of type 'event'
DEFAULT_EVENT_REWARD_VALUE = 0.001

