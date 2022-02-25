from datetime import datetime
import os
import random
import re
import string

import src.train.constants as tc
from src.ingest_firehose.constants import MODEL_NAME_REGEXP


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

    return f's3://{train_bucket_name}/rewarded_decisions/{model_name}/parquet/'


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
    if not re.match(MODEL_NAME_REGEXP, model_name):
        print(
            'Model name: {} failed to pass through the regex: {}'
            .format(model_name, MODEL_NAME_REGEXP))
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

    return re.sub(tc.DIGITS_DT_REGEXP, '', raw_dt_str)


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
    start_dt = get_start_dt()

    # assume
    # 10 chars for datetime-like string YYYYmmDDHHMMSS
    # 3 x `-` to separate <service>-<stage>-<model>-<time>
    # max 20 chars for service name
    # max 10 chars for stage
    # max 20 chars for model name

    service_name = os.getenv(tc.SERVICE_NAME_ENVVAR, None)
    stage = os.getenv(tc.STAGE_ENVVAR, None)

    train_job_name_elements = [service_name, stage, model_name]

    assert all([val is not None for val in train_job_name_elements])
    # this little syntactical nightmare is used in order to utilize list comprehension
    truncated_train_job_name_components = \
        [val[:max_chars] if val[-1:][0] != tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR else val[:max_chars - 1]
         for val, max_chars in zip(train_job_name_elements, [20, 10, 20])]

    truncated_train_job_name_components += [start_dt]

    raw_job_name = \
        tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR\
        .join([val for val in truncated_train_job_name_components if val])

    training_job_name = re.sub(tc.SPECIAL_CHARACTERS_REGEXP, '-', raw_job_name)
    assert len(training_job_name) <= 63
    return training_job_name
