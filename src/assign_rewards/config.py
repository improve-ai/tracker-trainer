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
AWS_BATCH_JOB_ARRAY_INDEX = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])

# The total number of jobs launched
REWARD_ASSIGNMENT_WORKER_COUNT = int(os.environ['REWARD_ASSIGNMENT_WORKER_COUNT'])

TRAIN_BUCKET = os.environ['TRAIN_BUCKET']

# Logging level
LOGGING_LEVEL = logging.INFO

# Logging format
LOGGING_FORMAT = '%(levelname)-5s: @%(funcName)-25s | %(message)s'

INCOMING_PATH = Path('/mnt/efs/incoming')

HISTORIES_PATH = Path('/mnt/efs/histories')

UNRECOVERABLE_PATH = Path('/mnt/efs/unrecoverable')

DEFAULT_REWARD_KEY = 'reward'

# Starting reward value for a decision record if it doesn't have one
DEFAULT_REWARD_VALUE = 0

# The default reward value of a record of type 'event'
DEFAULT_EVENTS_REWARD_VALUE = 0.001

