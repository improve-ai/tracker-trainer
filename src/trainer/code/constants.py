VALID_PARQUET_FILENAME = r"\d{8}T\d{6}Z-\d{8}T\d{6}Z-\d+-(.){36}.parquet"

WEIGHT_FEATURE_KEY = 'w'
TARGET_FEATURE_KEY = 'y'
TIMESTAMP_FEATURE_KEY = 't'
ITEM_FEATURE_KEY = 'item'
CONTEXT_FEATURE_KEY = 'context'

REWARD_KEY = 'reward'
ITEM_KEY = 'item'
CONTEXT_KEY = 'context'
COUNT_KEY = 'count'
SAMPLE_KEY = 'sample'

DECISION_ID_KEY = 'decision_id'
REWARDS_KEY = 'rewards'

DF_SCHEMA = {
    DECISION_ID_KEY : 'object',
    ITEM_KEY     : 'object',
    CONTEXT_KEY      : 'object',
    COUNT_KEY       : 'Int64',
    SAMPLE_KEY      : 'object',
    REWARDS_KEY     : 'object',
    REWARD_KEY      : 'float64',
}
