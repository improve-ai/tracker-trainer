import orjson
import os
import re
from pathlib import Path
import random

from utils import str2bool

VERSION = '8.0.0'

#
# Hardcoded Hyperparameters
#
TEST_SPLIT = 0.3
CLIP_MIN_PROPENSITY = 0.0001 # 1 in 10,000 decisions. This gives a maximum inverse propensity of 10,000
SAMPLE_CONTEXT = 0.95
REWARD_PRIOR_COUNT = 300
PRUNE_MIN_STRING_COUNT = 20

def should_sample_context():
    return random.random() < SAMPLE_CONTEXT

#
# Format Regexes
#
MODEL_NAME_REGEXP = "^[a-zA-Z0-9][\w\-.]{0,63}$"
VALID_PARQUET_FILENAME = r"\d{8}T\d{6}Z-\d{8}T\d{6}Z-\d+-(.){36}.parquet"

#
# Configuration Paths
#
SM_BASE_DIR = Path(os.getenv('SAGEMAKER_BASE_DIR', '/opt/ml/'))

MODEL_PATH = SM_BASE_DIR / 'model'
INPUT_PATH = SM_BASE_DIR / 'input'
INPUT_DATA_PATH = INPUT_PATH / 'data'
CONFIG_PATH = INPUT_PATH / 'config'
OUTPUT_PATH = SM_BASE_DIR / 'output'
FAILURE_MESSAGE_PATH = SM_BASE_DIR / 'failure'
CHECKPOINTS_PATH = SM_BASE_DIR / 'checkpoints'

REWARDED_DECISIONS_PATH = INPUT_DATA_PATH / 'decisions'

HYPERPARAMETER_PATH = CONFIG_PATH / 'hyperparameters.json'
with open(HYPERPARAMETER_PATH, 'r') as f:
    _read_hyperparameters = f.read()
    HYPERPARAMETERS = orjson.loads(_read_hyperparameters)

    
RESOURCE_CONFIG_PATH = CONFIG_PATH / 'resourceconfig.json'
with open(RESOURCE_CONFIG_PATH, 'r') as f:
    _read_resource_config = f.read()
    RESOURCE_CONFIG = orjson.loads(_read_resource_config)

# TODO this is the only usage of 'inputdataconfig.json'
INPUT_DATA_CONFIG_PATH = CONFIG_PATH / 'inputdataconfig.json'
with open(INPUT_DATA_CONFIG_PATH, 'r') as f:
    _read_input_data_config = f.read()
    INPUT_DATA_CONFIG = orjson.loads(_read_input_data_config)

ALL_HOSTNAMES = RESOURCE_CONFIG['hosts']
CURRENT_WORKER_HOSTNAME = RESOURCE_CONFIG['current_host']
MASTER_HOSTNAME = ALL_HOSTNAMES[0]
IS_MASTER = CURRENT_WORKER_HOSTNAME == MASTER_HOSTNAME

#
# MAX_DECISION_RECORDS
#
try:
    MAX_DECISION_RECORDS = int(HYPERPARAMETERS.get('max_decision_records', None))
except (ValueError, TypeError):
    MAX_DECISION_RECORDS = None

assert MAX_DECISION_RECORDS is None or MAX_DECISION_RECORDS > 0

#
# Model Name
#
MODEL_NAME = HYPERPARAMETERS.get('model_name')

if MODEL_NAME is None:
    raise ValueError('`model_name` is None')

if not isinstance(MODEL_NAME, str) \
        or len(MODEL_NAME) == 0 \
        or not re.match(MODEL_NAME_REGEXP, MODEL_NAME):
    raise ValueError('`model_name`: {} is invalid'.format(MODEL_NAME))

#
# User Configurable Hyperparameters
#
MAX_TREES = int(HYPERPARAMETERS.get('max_trees', 150))

MAX_FEATURES = int(HYPERPARAMETERS.get('max_features', 1000))

MAX_STRINGS_PER_FEATURE = int(HYPERPARAMETERS.get('max_strings_per_feature', 10000))

TREE_DEPTH = int(HYPERPARAMETERS.get('tree_depth', 6))

EXPLORE = str2bool(HYPERPARAMETERS.get('explore', 'true'))

NORMALIZE_REWARDS = str2bool(HYPERPARAMETERS.get('normalize_rewards', 'true'))

BINARY_REWARDS = str2bool(HYPERPARAMETERS.get('binary_rewards', 'false'))

# moved to config to have all HP entries in one place
MAX_CHECKPOINT_AGE_HYPERPARAMS_KEY = 'max_checkpoint_age'
MAX_CHECKPOINT_AGE_STRING = HYPERPARAMETERS.get(MAX_CHECKPOINT_AGE_HYPERPARAMS_KEY, None)

# dask worker config
CORES_PER_WORKER = 4
