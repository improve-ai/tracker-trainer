"""
Configuration file.
Store any constant or configuration variable of interest here.
-------------------------------------------------------------------------------
"""

# Built-in imports
from pathlib import Path
import logging
import os

# Local imports
from src.exceptions import EnvirontmentVariableError


try:
    # Absolute path towards the mounted EFS filesystem
    PATH_TOWARDS_EFS = Path(os.environ['PATH_TOWARDS_EFS'])

    # The length (in seconds) of the reward window
    REWARD_WINDOW = int(os.environ['DEFAULT_REWARD_WINDOW_IN_SECONDS'])

    # The worker number of this job
    AWS_BATCH_JOB_ARRAY_INDEX = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])

    # The total number of jobs launched
    JOIN_REWARDS_JOB_ARRAY_SIZE = int(os.environ['JOIN_REWARDS_JOB_ARRAY_SIZE'])

    # Determines if all input files should be reprocessed (and outputs deleted)
    JOIN_REWARDS_REPROCESS_ALL = os.environ['JOIN_REWARDS_REPROCESS_ALL'].lower()

except KeyError as e:
    print(e)
    raise EnvirontmentVariableError


# Logging level
LOGGING_LEVEL = logging.DEBUG

# Logging format
LOGGING_FORMAT = '%(levelname)-5s: @%(funcName)-25s | %(message)s'

# Absolute path towards the input folder
PATH_INPUT_DIR = PATH_TOWARDS_EFS / 'histories'

# Absolute path towards the output folder
PATH_OUTPUT_DIR = PATH_TOWARDS_EFS / 'rewarded_decisions'

# Output filename template of the gzipped results 
OUTPUT_FILENAME = '{}.gz'

DEFAULT_REWARD_KEY = 'default_reward'

# Starting reward value for a decision record if it doesn't have one
DEFAULT_REWARD_VALUE = 0

# The timestamp format of the records
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# The default reward value of a record of type 'event'
DEFAULT_EVENTS_REWARD_VALUE = 0.001

