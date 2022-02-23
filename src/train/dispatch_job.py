# Built-in imports
import os

# External imports
import boto3

# Local imports
import src.train.constants as tc
from src.train.naming import get_train_job_name, get_training_s3_uri_for_model, \
    get_s3_model_save_uri, is_valid_model_name


def create_sagemaker_training_job(
        sagemaker_client, hyperparameters: dict, event: dict):
    """
    Start a model training job.
    Refer to:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_training_job

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
        Training job ARN. E.g.: { 'TrainingJobArn': 'string' }
    """

    model_name = event[tc.EVENT_MODEL_NAME_KEY]

    role = os.getenv(tc.IAM_ROLE_EVVAR)

    image_uri = event[tc.EVENT_IMAGE_KEY]

    instance_count = event[tc.EVENT_WORKER_COUNT_KEY]
    instance_type = event[tc.EVENT_WORKER_INSTANCE_TYPE_KEY]
    volume_size = event[tc.EVENT_VOLUME_SIZE_KEY]

    subnets = [os.getenv(tc.SUBNET_ENVVAR)]
    security_groups_ids = \
        [os.getenv(tc.SECURITY_GROUP_BATCH_ENVVAR)]

    training_max_runtime = event[tc.EVENT_MAX_RUNTIME_KEY]

    training_job_name = get_train_job_name(model_name=model_name)
    training_s3_uri = get_training_s3_uri_for_model(model_name=model_name)
    model_save_s3_uri = get_s3_model_save_uri(model_name=model_name)

    response = sagemaker_client.create_training_job(
        TrainingJobName=training_job_name,
        HyperParameters=hyperparameters,
        AlgorithmSpecification={
            'TrainingImage': image_uri,
            'TrainingInputMode': 'FastFile'
        },
        RoleArn=role,
        InputDataConfig=[
            {
                'ChannelName': 'decisions',
                'InputMode': 'FastFile',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': training_s3_uri,
                        'S3DataDistributionType': 'FullyReplicated',
                    },
                }
            },
        ],
        ResourceConfig={
            'InstanceType': instance_type,
            'InstanceCount': instance_count,
            'VolumeSizeInGB': volume_size
        },
        VpcConfig={
            'SecurityGroupIds': security_groups_ids,
            'Subnets': subnets
        },
        StoppingCondition={
            'MaxRuntimeInSeconds': training_max_runtime,
        },
        OutputDataConfig={
            'S3OutputPath': model_save_s3_uri},
        EnableInterContainerTrafficEncryption=False
    )

    return response


def get_hyperparameters_for_model(model_name: str, event: dict):
    """
    Gets hyperparameter set for provided model name
    Parameters
    ----------
    model_name: str
        name of the model for which SageMaker's hyperparameter set should be
        returned
    event: Lambda function event object. Contains data about the event
           that triggered the Lambda function.
    Returns
    -------
    dict
        set of hyperparameters for training job of a <model name>
    """

    hyperparams = event.get(tc.HYPERPARAMETERS_KEY, {})
    hyperparams[tc.MODEL_NAME_HYPERPARAMS_KEY] = model_name

    return hyperparams


def check_train_job_properties(event: dict):
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

    check_train_job_properties(event=event)

    model_name = event.get(tc.EVENT_MODEL_NAME_KEY)
    assert is_valid_model_name(model_name=model_name)

    hyperparameters = get_hyperparameters_for_model(model_name, event)

    print(f'creating training job for model: {model_name}')
    response = \
        create_sagemaker_training_job(
            sagemaker_client=sagemaker_client, hyperparameters=hyperparameters, event=event)

    print('Sagemaker`s response was:')
    print(response)
