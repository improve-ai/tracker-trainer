IAM_ROLE_EVVAR = 'TRAINING_ROLE_ARN'
SUBNET_ENVVAR = 'TRAINING_SUBNET'
SECURITY_GROUP_BATCH_ENVVAR = 'TRAINING_SECURITY_GROUP'
TRAIN_BUCKET_ENVVAR = 'TRAIN_BUCKET'

MODELS_BUCKET_ENVVAR = 'MODELS_BUCKET'
MODEL_NAME_HYPERPARAMS_KEY = 'model_name'
HYPERPARAMETERS_KEY = 'hyperparameters'

SERVICE_NAME_ENVVAR = 'SERVICE_NAME'
STAGE_ENVVAR = 'STAGE'
REPOSITORY_NAME_ENVVAR = 'REPOSITORY_NAME'
IMAGE_TAG_ENVVAR = 'IMAGE_TAG'

SUBSCRIPTION_ENVVAR = 'SUBSCRIPTION'
DEFAULT_SUBSCRIPTION = 'free'
VALID_SUBSCRIPTIONS = [DEFAULT_SUBSCRIPTION, 'pro']

JOB_NAME_PREFIX = 'improve-train-job'


SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR = '-'
SPECIAL_CHARACTERS_REGEXP = "[^A-Za-z0-9-]"
DIGITS_DT_REGEXP = '\.|\-|:| '
SAGEMAKER_MAX_TRAIN_JOB_NAME_LENGTH = 63
# min 4 chars for stage
MIN_STAGE_LENGTH = 4
# min 8 chars for model name
MIN_MODEL_NAME_LENGTH = 8

EVENT_MODEL_NAME_KEY = 'model_name'
EVENT_WORKER_INSTANCE_TYPE_KEY = 'instance_type'
EVENT_WORKER_COUNT_KEY = 'instance_count'
EVENT_MAX_RUNTIME_KEY = 'max_runtime'
EVENT_VOLUME_SIZE_KEY = 'volume_size'
EVENT_IMAGE_KEY = 'image'

EXPECTED_EVENT_ENTRIES = [EVENT_MODEL_NAME_KEY, EVENT_WORKER_INSTANCE_TYPE_KEY, EVENT_WORKER_COUNT_KEY, EVENT_MAX_RUNTIME_KEY]

LOCAL_CHECKPOINT_PATH = '/opt/ml/checkpoints'

# IMAGE_URI_REGEXP chunks:
# - 12 digits in front for AWS account ID
# - .dkr.ecr. is always present in private ECR URI
# - after that comes region name which should be between 9 and 14 alnum + ['-'] characters:
#   - shortest region name is e.g. 'us.east-1'
#   - longest is e.g. 'ap-southeast-4'
# - .amazonaws.com is always present in private ECR URI before '/<ecr repo name>:<image tag>
IMAGE_URI_REGEXP = '^\d{12}\.dkr\.ecr\.[a-zA-Z0-9\-]{9,14}\.amazonaws.com'

# expect: <account ID>,dkr.ecr.<region>.amazonaws.com/<repository name>:<tag>
IMAGE_URI_PATTERN = '{}.dkr.ecr.{}.amazonaws.com/{}:{}'
