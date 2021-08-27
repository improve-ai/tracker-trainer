from datetime import datetime
import os
import random
import re
import string

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
    if not is_valid_model_name(model_name):
        raise ValueError(f'invalid model name {model_name}')
        
    train_bucket_name = os.environ[tc.TRAIN_BUCKET_ENVVAR]

    return f's3://{train_bucket_name}/rewarded_decisions/{model_name}'


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
    if not is_valid_model_name(model_name):
        raise ValueError(f'invalid model name {model_name}')

    train_bucket_name = os.environ[tc.TRAIN_BUCKET_ENVVAR]

    return f's3://{train_bucket_name}/train_output/{model_name}'

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
        print(
            'Model name: {} failed to pass through the regex: {}'
            .format(model_name, tc.MODEL_NAME_REGEXP))
        return False
    return True


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


def generate_random_string(size, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def get_train_job_name(model_name: str) -> str:
    """
    Creates train job name for sagemaker

    Parameters
    ----------
    model_name: str
        name of model to be trained

    Returns
    -------
    str
        name of SageMaker train job

    """
    start_dt = get_start_dt()[2:]

    # assume
    # 8 chars for datetime-like string YYmmDDHHMMSS
    # min 4 random alnum chars
    # 3 x `-` to separate <service>-<model name>-<dt string>-<random chars>
    # max 28 chars for service name
    # max 20 chars for model name

    truncated_service_name = os.getenv(tc.SERVICE_NAME_ENVVAR)[:28]
    if truncated_service_name[-1] == tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR:
        truncated_service_name = truncated_service_name[:-1]

    truncated_model_name = model_name[:20]
    if truncated_model_name[-1] == tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR:
        truncated_model_name = truncated_model_name[:-1]

    random_remainder_size = \
        tc.SAGEMAKER_TRAIN_JOB_NAME_LENGTH - \
        (len(truncated_service_name) + len(truncated_model_name) + len(start_dt) + 3)

    random_remainder = generate_random_string(random_remainder_size)

    raw_job_name = \
        tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR\
        .join([truncated_service_name, truncated_model_name, start_dt + random_remainder])

    return re.sub(tc.SPECIAL_CHARACTERS_REGEXP, '-', raw_job_name)
