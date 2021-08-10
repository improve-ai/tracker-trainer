from datetime import datetime
import os
import re

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
