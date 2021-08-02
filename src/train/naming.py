from datetime import datetime
import os
import re
import yaml

import src.train.constants as tc


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

    training_bucket_name = os.getenv(tc.TRAINING_INPUT_BUCKET_ENVVAR)

    return \
        tc.AWS_BUCKET_PREFIX + tc.AWS_S3_PATH_SEP.join(
            [training_bucket_name, tc.TRAINING_INPUT_BUCKET_SUBDIR, model_name])


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
    models_bucket_name = os.getenv(tc.MODELS_BUCKET_ENVVAR)

    return \
        tc.AWS_BUCKET_PREFIX + tc.AWS_S3_PATH_SEP.join(
            [models_bucket_name, tc.MODELS_BUCKET_SUBDIR, model_name])


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
    if not re.match(tc.MODEL_NAME_REGEXP, model_name):
        print('Model name failed to pass through the regex')
        return False
    return True


def get_model_names_from_config():
    """
    Parses config to extract model names from it

    Returns
    -------
    list
        list of model names extracted from config

    """
    # load config
    with open(tc.CONFIG_YAML_PATH, 'r') as cfg_yml:
        config = yaml.full_load(cfg_yml)

    # get models config
    models_config = config.get('models')
    # get model names
    model_names = models_config.keys()

    valid_model_names = [mn for mn in model_names if is_valid_model_name(mn)]
    invalid_model_names = \
        [mn for mn in model_names if not is_valid_model_name(mn)]

    print('Valid model names from config: {}'.format(valid_model_names))
    if invalid_model_names:
        print('Invalid model names from config: {}'.format(invalid_model_names))

    return valid_model_names


def get_model_names_from_s3(s3_client, bucket_name: str) -> list:
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
        [el.split(tc.AWS_S3_PATH_SEP)[0] == tc.TRAINING_INPUT_BUCKET_SUBDIR
         for el in model_subdirectories])
    valid_model_names = \
        [el.split(tc.AWS_S3_PATH_SEP)[1] for el in model_subdirectories
         if is_valid_model_name(el.split(tc.AWS_S3_PATH_SEP)[1])]

    print('Valid S3 model names: {}'.format(valid_model_names))

    print('Invalid S3 model names: {}'.format(
        [el.split(tc.AWS_S3_PATH_SEP)[1] for el in model_subdirectories
         if not is_valid_model_name(el.split(tc.AWS_S3_PATH_SEP)[1])]))
    return valid_model_names


def get_model_names(s3_client) -> list:
    bucket_name = os.getenv(tc.TRAINING_INPUT_BUCKET_ENVVAR)
    s3_model_names = \
        get_model_names_from_s3(s3_client=s3_client, bucket_name=bucket_name)
    config_model_names = get_model_names_from_config()

    models_with_data = [mn for mn in config_model_names if mn in s3_model_names]
    models_without_data = \
        [mn for mn in config_model_names if mn not in s3_model_names]

    print(
        'Model names from config with data in s3: {}'.format(models_with_data))
    if models_without_data:
        print(
            'Model names from config without data in s3: {}'
            .format(models_without_data))

    return models_with_data


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

    return '{}-{}-{}'.format(
        tc.JOB_NAME_PREFIX, model_name, start_dt).replace('.', '-')
