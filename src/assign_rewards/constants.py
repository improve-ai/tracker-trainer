MESSAGE_ID_KEY = 'message_id'
HISTORY_ID_KEY = 'history_id'
TIMESTAMP_KEY = 'timestamp'
TYPE_KEY = 'type'
DECISION_TYPE = 'decision'
EVENT_TYPE = 'event'
MODEL_KEY = 'model'
VARIANT_KEY = 'variant'
GIVEN_KEY = 'given'
COUNT_KEY = 'count'
SAMPLE_KEY = 'sample'
RUNNERS_UP_KEY = 'runners_up'
PROPERTIES_KEY = 'properties'
VALUE_KEY = 'value'

MODEL_NAME_REGEXP = "^[\w\- .]+$"
HISTORY_FILE_NAME_REGEXP = '^[\w]+$'
UUID4_FILE_NAME_REGEXP = "^[\w\-]+$"
JSONLINES_FILENAME_EXTENSION_REGEXP = '.*\.jsonl.gz$'


REWARDED_DECISION_KEYS = \
    ('timestamp', 'reward', 'variant', 'given', 'count', 'runners_up', 'sample')

SHA_256_LEN = 64
UUID4_LENGTH = 36
