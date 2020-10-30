"""
Configuration file.
Store any constant or configuration variable of interest here.
-------------------------------------------------------------------------------
"""

from pathlib import Path
import logging

# Logging level
LOGGING_LEVEL = logging.DEBUG

# Logging format
LOGGING_FORMAT = '%(levelname)-5s: @%(funcName)-25s | %(message)s'

# Absolute path towards the mounted EFS filesystem
PATH_TOWARDS_EFS = Path('./src')

# Absolute path towards the input folder
PATH_INPUT_DIR = PATH_TOWARDS_EFS / 'histories'

# Absolute path towards the output folder
PATH_OUTPUT_DIR = PATH_TOWARDS_EFS / 'rewarded_decision'

# Output filename template of the gzipped results 
OUTPUT_FILENAME = '{}.gz'

DEFAULT_REWARD_KEY = 'default_reward'

# Starting reward value for a decision record if it doesn't have one
DEFAULT_REWARD_VALUE = 0

# The timestamp format of the records
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# The default reward value of a record of type 'event'
DEFAULT_EVENTS_REWARD_VALUE = 0.001

