import boto3
import os

import src.train.constants as tc
from src.train.naming import get_train_job_name, get_training_s3_uri_for_model, \
    get_s3_model_save_uri, get_model_names


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

    role = os.getenv(tc.IAM_ROLE_EVVAR)

    image_uri = os.getenv(tc.IMAGE_URI_ENVVAR)

    training_input_channel_name = tc.TRAINING_INPUT_CHANNEL_NAME
    training_input_channel_mode = tc.TRAINING_INPUT_MODE
    training_input_channel_compression = tc.TRAINING_INPUT_COMPRESSION
    training_input_sharding_type = tc.TRAINING_SHARDING_TYPE

    instance_count = int(os.getenv(tc.INSTANCE_COUNT_ENVVAR))
    instance_type = os.getenv(tc.INSTANCE_TYPE_ENVVAR)
    subnets = [os.getenv(tc.SUBNET_ENVVAR)]
    security_groups_ids = \
        [os.getenv(tc.SECURITY_GROUP_BATCH_ENVVAR)]

    training_max_runtime_s = int(os.getenv(tc.MAX_RUNTIME_IN_SECONDS_ENVVAR))

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
                    'Seed': tc.SHUFFLE_INPUT_SEED
                }
            },
        ],
        ResourceConfig={
            'InstanceType': instance_type,
            'InstanceCount': instance_count,
            'VolumeSizeInGB': tc.VOLUME_SIZE,
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
            tc.MODEL_NAME_ENVVAR: model_name
        },
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
    return {}


def lambda_handler(event, context):

    # get sagemaker_client
    s3_client = boto3.client("s3")
    sagemaker_client = boto3.client('sagemaker')

    # get all model names
    model_names = get_model_names(s3_client=s3_client)

    if not model_names:
        print(
            'No valid model names found in the training bucket: {}'
            .format(os.getenv(tc.TRAINING_INPUT_BUCKET_ENVVAR)))
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
