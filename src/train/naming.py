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

    service_name = os.getenv(tc.SERVICE_NAME_ENVVAR, None)
    stage = os.getenv(tc.STAGE_ENVVAR, None)

    train_job_name_elements = [service_name, stage, model_name, start_dt]

    assert all([val is not None for val in train_job_name_elements])

    initial_job_name = \
        tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR.join(
            [val for val in train_job_name_elements if val])

    if len(initial_job_name) <= 63:
        return initial_job_name

    # if full job name components form a job name which is longer than 63 characters
    # (max length allowed by SageMaker) then allow:
    # extract lengths
    service_name_length = len(service_name)
    # Ensure a minimum of 8 chars can fit in the model name.
    # Ensure a minimum of 4 chars for the stage - truncate the end of service to accomplish that if necessary.
    # Only truncate the stage as is required to fit into 63 characters and meeting the minimum character requirements in the description.
    # check how many characters remain once service name and datetime is subtracted from
    separators_count = len([val for val in train_job_name_elements if val]) - 1
    # 63 is max for train job name, 4 is min for model name, 8 is
    remaining_chars = \
        tc.SAGEMAKER_MAX_TRAIN_JOB_NAME_LENGTH - separators_count - tc.MIN_STAGE_LENGTH - tc.MIN_MODEL_NAME_LENGTH - \
        len(start_dt) - service_name_length
    # if remaining_chars is negative it means that service_name should be trimmed
    truncated_service_name = service_name[:remaining_chars] if remaining_chars < 0 else service_name
    # length of model_name and stage should be determined
    extra_chars_model_name = \
        0 if remaining_chars < 0 else (
            int(remaining_chars / 2) if remaining_chars % 2 == 0 else int(remaining_chars / 2) + 1)
    extra_chars_stage = int(remaining_chars / 2) if remaining_chars > 0 else 0
    truncated_model_name = model_name[:8 + extra_chars_model_name]
    truncated_stage = stage[:4 + extra_chars_stage]

    truncated_train_job_name_components = [truncated_service_name, truncated_stage, truncated_model_name, start_dt]
    initial_truncated_job_name = \
        tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR\
        .join([val for val in truncated_train_job_name_components if val])

    training_job_name = re.sub(tc.SPECIAL_CHARACTERS_REGEXP, '-', initial_truncated_job_name)
    assert len(training_job_name) <= 63
    return training_job_name
