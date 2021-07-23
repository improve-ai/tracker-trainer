import boto3
from datetime import datetime
import os
import re


SHUFFLE_INPUT_SEED = 0
VOLUME_SIZE = 3  # ?
IMPROVE_VERSION = 'v6'
JOB_NAME_PREFIX = 'improve-train-job-{}'.format(IMPROVE_VERSION)
MODEL_NAME_REGEXP = "^[\w\- .]+$"
AWS_BUCKET_PREFIX = 's3://'
AWS_S3_PATH_SEP = '/'
MODEL_NAME_ENVVAR = 'MODEL_NAME'


MAX_RUNTIME_IN_SECONDS_ENVVAR = 'TRAINING_MAX_RUNTIME_IN_SECONDS'
INSTANCE_COUNT_ENVVAR = 'TRAINING_INSTANCE_COUNT'
INSTANCE_TYPE_ENVVAR = 'TRAINING_INSTANCE_TYPE'
IMAGE_URI_ENVVAR = 'TRAINING_IMAGE'

IAM_ROLE_EVVAR = 'TRAINING_ROLE_ARN'
SUBNET_ENVVAR = 'TRAINING_SUBNET'
SECURITY_GROUP_BATCH_ENVVAR = 'TRAINING_SECURITY_GROUP'

TRAINING_INPUT_BUCKET_ENVVAR = 'TRAIN_BUCKET'
TRAINING_INPUT_BUCKET_SUBDIR = 'rewarded_decisions'

TRAINING_INPUT_CHANNEL_NAME_ENVVAR = 'TRAINING_INPUT_CHANNEL_NAME'
TRAINING_INPUT_CHANNEL_MODE_ENVVAR = 'TRAINING_INPUT_MODE'
TRAINING_INPUT_CHANNEL_COMPRESSION_ENVVAR = 'TRAINING_INPUT_COMPRESSION'
TRAINING_SHARDING_TYPE_ENVVAR = 'TRAINING_SHARDING_TYPE'


MODELS_BUCKET_ENVVAR = 'MODELS_BUCKET'
MODELS_BUCKET_SUBDIR = 'models'


def get_training_s3_uri_for_model(model_name: str):
    """
    Gets S3 uri using info from ENV

    Parameters
    ----------
    model_name: str
        name of the model which will be trained

    Returns
    -------
    str
        URI to S3 resource which will be used as an input for <model name>
        training

    """

    training_bucket_name = os.getenv(TRAINING_INPUT_BUCKET_ENVVAR)

    return \
        AWS_BUCKET_PREFIX + AWS_S3_PATH_SEP.join(
            [training_bucket_name, TRAINING_INPUT_BUCKET_SUBDIR, model_name])


def get_s3_model_save_uri(model_name: str):
    """
    Helper - gets uri of model save S3 location

    Parameters
    ----------
    model_name: str
        name of model which is trained

    Returns
    -------
    str
        S3 uri for model save

    """
    models_bucket_name = os.getenv(MODELS_BUCKET_ENVVAR)

    return \
        AWS_BUCKET_PREFIX + AWS_S3_PATH_SEP.join(
            [models_bucket_name, MODELS_BUCKET_SUBDIR, model_name])


def get_s3_bucket_contents_paths(s3_client, bucket_name: str) -> list:
    """
    Helper - gets contents of desired S3 bucket

    Parameters
    ----------
    s3_client: boto3.client
        s3 client object
    bucket_name: str
        name of bucket to be listed

    Returns
    -------
    list
        list with S3 bucket contents

    """

    contents = \
        s3_client.list_objects(Bucket=bucket_name).get('Contents', None)

    if not contents:
        return []

    bucket_contents_paths = [el.get('Key', None) for el in contents]

    return bucket_contents_paths


def is_valid_model_name(model_name: str) -> bool:
    """
    Helper - validates model name

    Parameters
    ----------
    model_name: str
        name of model to be validated

    Returns
    -------
    bool
        is the model name valid?

    """
    if not re.match(MODEL_NAME_REGEXP, model_name):
        print('Model name failed to pass through the regex')
        return False
    return True


def create_sagemaker_training_job(
        sagemaker_client,  model_name: str, hyperparameters: dict):
    """
    Gets boto3 sagemaker api (client.create_training_job()) kwargs with info
    from environment

    Parameters
    ----------
    sagemaker_client: boto3.client
        boto3 sagemaker client
    model_name: str
        name of model which will be trained
    hyperparameters: dict
        hyperparameters for desired model train job

    Returns
    -------
    dict
        kwargs for create_training_job() call

    """

    role = os.getenv(IAM_ROLE_EVVAR)

    image_uri = os.getenv(IMAGE_URI_ENVVAR)

    training_input_channel_name = \
        os.getenv(TRAINING_INPUT_CHANNEL_NAME_ENVVAR)

    training_input_channel_mode = \
        os.getenv(TRAINING_INPUT_CHANNEL_MODE_ENVVAR)

    training_input_channel_compression = \
        os.getenv(TRAINING_INPUT_CHANNEL_COMPRESSION_ENVVAR)

    training_input_sharding_type = os.getenv(TRAINING_SHARDING_TYPE_ENVVAR)

    instance_count = int(os.getenv(INSTANCE_COUNT_ENVVAR))
    instance_type = os.getenv(INSTANCE_TYPE_ENVVAR)
    subnets = [os.getenv(SUBNET_ENVVAR)]
    security_groups_ids = \
        [os.getenv(SECURITY_GROUP_BATCH_ENVVAR)]

    training_max_runtime_s = int(os.getenv(MAX_RUNTIME_IN_SECONDS_ENVVAR))

    training_job_name = get_train_job_name(model_name=model_name)
    training_s3_uri = get_training_s3_uri_for_model(model_name=model_name)
    model_save_s3_uri = get_s3_model_save_uri(model_name=model_name)

    response = sagemaker_client.create_training_job(
        TrainingJobName=training_job_name,
        HyperParameters=hyperparameters,
        AlgorithmSpecification={
            'TrainingImage': image_uri,
            'TrainingInputMode': training_input_channel_mode
        },
        RoleArn=role,
        InputDataConfig=[
            {
                'ChannelName': training_input_channel_name,
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': training_s3_uri,
                        'S3DataDistributionType':
                            training_input_sharding_type
                    },
                },
                # 'ContentType': content_type,
                'CompressionType': training_input_channel_compression,
                'InputMode': training_input_channel_mode,
                'ShuffleConfig': {
                    'Seed': SHUFFLE_INPUT_SEED
                }
            },
        ],
        ResourceConfig={
            'InstanceType': instance_type,
            'InstanceCount': instance_count,
            'VolumeSizeInGB': VOLUME_SIZE,
        },
        VpcConfig={
            'SecurityGroupIds': security_groups_ids,
            'Subnets': subnets
        },
        StoppingCondition={
            'MaxRuntimeInSeconds': training_max_runtime_s,
        },
        OutputDataConfig={
            'S3OutputPath': model_save_s3_uri},
        Environment={
            MODEL_NAME_ENVVAR: model_name
        },
        EnableInterContainerTrafficEncryption=False
    )

    return response


def get_model_names(s3_client, bucket_name: str) -> list:
    """
    Lists subfolders of <train bucket>/<bucket subdirectory>
    (e.g. improve-acme-train/rewarded_decisions) bucket and returns valid model
    names from this list

    Parameters
    ----------
    bucket_name: str
        ARN of the bucket holding train data for improve models

    Returns
    -------
    list
        list of valid model names found in the S3 bucket

    """

    model_subdirectories = \
        get_s3_bucket_contents_paths(
            s3_client=s3_client, bucket_name=bucket_name)
    # validate per model paths

    if not model_subdirectories:
        return []

    assert all(
        [el.split(AWS_S3_PATH_SEP)[0] == TRAINING_INPUT_BUCKET_SUBDIR
         for el in model_subdirectories])
    valid_model_names = \
        [el.split(AWS_S3_PATH_SEP)[1] for el in model_subdirectories
         if is_valid_model_name(el.split(AWS_S3_PATH_SEP)[1])]

    print('Valid model names: {}'.format(valid_model_names))

    print('Invalid model names: {}'.format(
        [el.split(AWS_S3_PATH_SEP)[1] for el in model_subdirectories
         if not is_valid_model_name(el.split(AWS_S3_PATH_SEP)[1])]))
    return valid_model_names


def get_start_dt() -> str:
    """
    Helper function - leaves only digits in datetime and returns as string

    Returns
    -------
    str
        only digits from datetime

    """
    raw_dt_str = str(datetime.now()).split('.')[0]

    return re.sub('\.|\-|:|\s', '', raw_dt_str)


# TODO ask how this should be created (?)
def get_train_job_name(model_name: str) -> str:
    """
    Creates train job name for sagemaker call using datetime of train job start
    and model name

    Parameters
    ----------
    model_name: str
        name of model to be trained

    Returns
    -------
    str
        name of SageMaker train job

    """
    start_dt = get_start_dt()

    return '{}-{}-{}'.format(JOB_NAME_PREFIX, model_name, start_dt)


def get_hyperparameters_for_model(model_name: str):
    """
    Gets hyperparameter set for provided model name

    Parameters
    ----------
    model_name: str
        name of the model for which SageMaker's hyperparameter set should be
        returned

    Returns
    -------
    dict
        set of hyperparameters for training job of a <model name>

    """
    return {}


def lambda_handler(event, context):

    # get sagemaker_client
    s3_client = boto3.client("s3")
    sagemaker_client = boto3.client('sagemaker')

    # get all model names
    bucket_name = os.getenv(TRAINING_INPUT_BUCKET_ENVVAR)
    model_names = get_model_names(s3_client=s3_client, bucket_name=bucket_name)

    if not model_names:
        print(
            'No valid model names found in the training bucket: {}'
            .format(os.getenv(TRAINING_INPUT_BUCKET_ENVVAR)))
        return

    successful_starts = []
    failed_starts = []

    print('Found train data for: {}'.format(model_names))

    for model_name in model_names:

        print('Getting hyperparameters for model: {}'.format(model_name))
        hyperparameters = get_hyperparameters_for_model(model_name=model_name)

        try:
            print('Creating training job for model: {}'.format(model_name))
            response = \
                create_sagemaker_training_job(
                    sagemaker_client=sagemaker_client, model_name=model_name,
                    hyperparameters=hyperparameters)
            print('Sagemaker`s response was:')
            print(response)
            successful_starts.append(model_name)
        except Exception as exc:
            print(
                'When attempting to run training job for model: {} the '
                'following exception occurred:'.format(model_name))
            print(exc)
            failed_starts.append(model_name)

    # TODO maybe check train_jobs_processes for completion
    print('Attempt to dispatch all train jobs complete:')
    print(' - train jobs for models: {} started successfully'
          .format(successful_starts))
    if failed_starts:
        print(' - train jobs for models {} failed to start'.format(failed_starts))
