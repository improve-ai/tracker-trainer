from datetime import datetime
import os
import random
import re
import string

import boto3

import src.train.constants as tc
from src.ingest.constants import MODEL_NAME_REGEXP


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

    return f's3://{train_bucket_name}/train_output/models/{model_name}'


def get_checkpoints_s3_uri(model_name):
    if not is_valid_model_name(model_name):
        raise ValueError(f'invalid model name {model_name}')

    train_bucket_name = os.environ[tc.TRAIN_BUCKET_ENVVAR]

    return f's3://{train_bucket_name}/train_output/checkpoints/{model_name}'


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
        tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR.join([val for val in train_job_name_elements if val])

    if len(initial_job_name) <= 63:
        train_job_name = re.sub(tc.SPECIAL_CHARACTERS_REGEXP, '-', initial_job_name)
        return train_job_name

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

    # if stage is empty add MIN_STAGE_LENGTH to remaining_chars
    if len(stage) == 0:
        remaining_chars += tc.MIN_STAGE_LENGTH
    # if remaining_chars is negative it means that service_name should be trimmed
    truncated_service_name = service_name[:remaining_chars] if remaining_chars < 0 else service_name

    remaining_chars_denominator = 1 if len(stage) == 0 else 2
    # length of model_name and stage should be determined
    extra_chars_model_name = \
        0 if remaining_chars < 0 else (
            int(remaining_chars / remaining_chars_denominator) if remaining_chars % 2 == 0
            else int(remaining_chars / remaining_chars_denominator) + 1)

    extra_chars_stage = int(remaining_chars / remaining_chars_denominator) if remaining_chars > 0 else 0

    truncated_model_name = model_name[:8 + extra_chars_model_name]
    truncated_stage = stage[:4 + extra_chars_stage]

    truncated_train_job_name_components = [truncated_service_name, truncated_stage, truncated_model_name, start_dt]
    initial_truncated_job_name = \
        tc.SAGEMAKER_TRAIN_JOB_NAME_SEPARATOR.join([val for val in truncated_train_job_name_components if val])

    training_job_name = re.sub(tc.SPECIAL_CHARACTERS_REGEXP, '-', initial_truncated_job_name)
    assert len(training_job_name) <= 63
    return training_job_name


def get_image_uri():
    """
    Using env vars construct a full image URI to be used for the training job.
    Assumes that trainer image is in the private repo deployed along with
    tracker-trainer platform
    Looks for following variables in environment:
    - 'REPOSITORY_NAME' (tc.REPOSITORY_NAME_ENVVAR) -> repository name
    - 'IMAGE_TAG' (tc.IMAGE_TAG_ENVVAR) -> image tag

    Returns
    -------
    str
        trainer image URI

    """
    repository_name = os.getenv(tc.REPOSITORY_NAME_ENVVAR, None)
    assert repository_name is not None

    image_tag = os.getenv(tc.IMAGE_TAG_ENVVAR, None)
    assert image_tag is not None

    # get account ID
    sts = boto3.client('sts')
    caller_identity_response = sts.get_caller_identity()
    assert 'Account' in caller_identity_response, \
        'In `get_caller_identity()` there is `Account` key missing'
    account_id = caller_identity_response['Account']

    region = boto3.session.Session().region_name
    assert region is not None

    # expect: <account ID>,dkr.ecr.<region>.amazonaws.com/<repository name>:<tag>
    image_uri = tc.IMAGE_URI_PATTERN.format(account_id, region, repository_name,
                                            image_tag)
    assert re.search(tc.IMAGE_URI_REGEXP, image_uri)

    return image_uri
