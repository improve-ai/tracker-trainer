"""
Configuration file.
Store any constant or configuration variable of interest here.
-------------------------------------------------------------------------------
"""

REWARD_WINDOW_SECONDS = 3600 * 2

# Input filename (for local testing)
INPUT_FILENAME = 'improve-v5-resources-prod-firehose-1-2020-10-19-03-59-11-ef716009-8325-4e98-befa-d5b5090ba24e'

# Output filename of the gzipped results
OUTPUT_FILENAME = '{}.jsonl.gz'

DEFAULT_REWARD_KEY = 'default_reward'

# Starting reward value for a decision record if it doesn't have one
DEFAULT_REWARD_VALUE = 0

# The timestamp format of the records
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# The default reward value of a record of type 'event'
DEFAULT_EVENTS_REWARD_VALUE = 0.001