import boto3
import os
import random

import src.train.constants as tc
from src.train.naming import get_train_job_name, get_training_s3_uri_for_model, \
    get_s3_model_save_uri, is_valid_model_name


def create_sagemaker_training_job(
        sagemaker_client, hyperparameters: dict, event: dict):
    """
    Gets boto3 sagemaker api (client.create_training_job()) kwargs with info
    from environment

    Parameters
    ----------
    sagemaker_client: boto3.client
        boto3 sagemaker client
    hyperparameters: dict
        hyperparameters for desired model train job
    event: dict
        event dict provided by scheduler

    Returns
    -------
    dict
        kwargs for create_training_job() call

    """

    model_name = event[tc.EVENT_MODEL_NAME_KEY]

    role = os.getenv(tc.IAM_ROLE_EVVAR)

    image_uri = os.getenv(tc.IMAGE_URI_ENVVAR)

    instance_count = event[tc.EVENT_WORKER_COUNT_KEY]
    instance_type = event[tc.EVENT_WORKER_INSTANCE_TYPE_KEY]

    subnets = [os.getenv(tc.SUBNET_ENVVAR)]
    security_groups_ids = \
        [os.getenv(tc.SECURITY_GROUP_BATCH_ENVVAR)]

    training_max_runtime_s = event[tc.EVENT_MAX_RUNTIME_IN_SECONDS_KEY]

    training_job_name = get_train_job_name(model_name=model_name)
    training_s3_uri = get_training_s3_uri_for_model(model_name=model_name)
    model_save_s3_uri = get_s3_model_save_uri(model_name=model_name)

    response = sagemaker_client.create_training_job(
        TrainingJobName=training_job_name,
        HyperParameters=hyperparameters,
        AlgorithmSpecification={
            'TrainingImage': image_uri,
            'TrainingInputMode': 'Pipe'
        },
        RoleArn=role,
        InputDataConfig=[
            {
                'ChannelName': 'decisions',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': training_s3_uri,
                        'S3DataDistributionType': 'ShardedByS3Key'
                    },
                },
                'CompressionType': 'Gzip',
                'ShuffleConfig': {
                    'Seed': random.randint(0, int(1e9))
                }
            },
        ],
        ResourceConfig={
            'InstanceType': instance_type,
            'InstanceCount': instance_count,
            'VolumeSizeInGB': tc.VOLUME_SIZE_IN_GB,
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
        EnableInterContainerTrafficEncryption=False
    )

    return response


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
    return {
        tc.MODEL_NAME_HYPERPARAMS_KEY: model_name
    }


def check_v6_train_job_properties(event: dict):
    """
    Checks if event dict contains all expected / desired keys and that they are
    not None

    Parameters
    ----------
    event: dict

    """

    job_parameters = [event.get(param, None) for param in tc.EXPECTED_EVENT_ENTRIES]

    for param_name, param_value in zip(tc.EXPECTED_EVENT_ENTRIES, job_parameters):
        if param_value is None:
            raise ValueError(
                '`{}` parameter must be provided and must not be None'.format(param_name))


def lambda_handler(event, context):

    sagemaker_client = boto3.client('sagemaker')

    check_v6_train_job_properties(event=event)

    model_name = event.get(tc.EVENT_MODEL_NAME_KEY)
    assert is_valid_model_name(model_name=model_name)

    hyperparameters = get_hyperparameters_for_model(model_name=model_name)

    print(f'creating training job for model: {model_name}')
    response = \
        create_sagemaker_training_job(
            sagemaker_client=sagemaker_client, hyperparameters=hyperparameters, event=event)

    print('Sagemaker`s response was:')
    print(response)
