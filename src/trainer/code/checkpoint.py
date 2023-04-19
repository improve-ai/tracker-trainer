from datetime import datetime, timedelta

import orjson
import re
import xgboost as xgb

from config import VERSION, CHECKPOINTS_PATH, MAX_CHECKPOINT_AGE_STRING
from feature_encoder import FeatureEncoder
from model_utils import CREATED_AT_METADATA_KEY, FEATURE_NAMES_METADATA_KEY, \
    MEAN_ITEM_COUNT_METADATA_KEY, MODEL_SEED_METADATA_KEY, STRING_TABLES_METADATA_KEY, \
    USER_DEFINED_METADATA_KEY, VERSION_METADATA_KEY
from model_utils import append_metadata_to_booster


ALLOWED_MAX_CHECKPOINT_AGE_UNITS = {
    'seconds': 'seconds',
    'minutes': 'minutes',
    'hours': 'hours',
    'days': 'days',
    'second': 'seconds',
    'minute': 'minutes',
    'hour': 'hours',
    'day': 'days'}


def load_checkpoint():
    """
    Load desired checkpoint model either for phase 1 or 2

    Returns
    -------
    tuple
        A tuple of phase 1 booster, trainer's FeatureEncoder and mean_item_count.
        Those 3 objects are needed to instantiate PropensityModel object needed
        for training's final phase

    """

    # create booster object for checkpointed model
    checkpoint_booster = xgb.Booster()
    loaded_checkpoint_path = CHECKPOINTS_PATH / 'phase1.xgb'

    try:
        # attempt to load model from checkpoint file
        checkpoint_booster.load_model(loaded_checkpoint_path)
        # extract feature names from checkpoint metadata
        checkpoint_booster_metadata = orjson.loads(checkpoint_booster.attr(USER_DEFINED_METADATA_KEY))
        feature_names = checkpoint_booster_metadata[FEATURE_NAMES_METADATA_KEY]
        # add feature names to booster so it will work
        checkpoint_booster.feature_names = feature_names
    except xgb.core.XGBoostError as xgberr:
        print(f'XGBoost`s Error when attempting to read: {loaded_checkpoint_path}')
        print(xgberr)
        # return None on xgb.core.XGBoostError -> checkpoint requires retraining
        return None
    except orjson.JSONDecodeError as jsonerr:
        # TODO test this path?
        print(f'Error when loading checkpoint metadata: {loaded_checkpoint_path}')
        print(jsonerr)
        # return None on xgb.core.XGBoostError -> checkpoint requires retraining
        return None

    try:
        assert checkpoint_booster_metadata is not None

        if checkpoint_booster_metadata.get(VERSION_METADATA_KEY) != VERSION:
            print('version mismatch between checkpoint model and trainer image, skipping checkpoint')
            return None

        created_at_string = checkpoint_booster_metadata.get(CREATED_AT_METADATA_KEY, None)
        assert created_at_string is not None

        if not use_checkpoint(datetime.fromisoformat(created_at_string)):
            return None

        mean_item_count = checkpoint_booster_metadata.get(MEAN_ITEM_COUNT_METADATA_KEY, None)
        assert mean_item_count is not None

        feature_encoder = get_feature_encoder_from_checkpoint(metadata=checkpoint_booster_metadata)
    except Exception as exc:
        print('Error while creating FeatureEncoder from metadata:')
        print(exc)
        # return None on xgb.core.XGBoostError -> checkpoint requires retraining
        return None
    return checkpoint_booster, feature_encoder, mean_item_count


def use_checkpoint(created_at) -> bool:
    """
    Does timediff from now to most recent checkpoint creation date exceede max_checkpoint_age
    Parameters
    ----------
    created_at: datetime
        datetime indicating when

    Returns
    -------
    bool
        True if checkpoint should be used
    """
    assert isinstance(created_at, datetime)
    if (datetime.now() - created_at) > parse_max_checkpoint_age(MAX_CHECKPOINT_AGE_STRING):
        return False
    return True


def save_xgboost_checkpoint(booster, string_tables, model_seed, phase_index: int, mean_item_count=None):

    """
    Saves checkpoint for desired phase of training

    Parameters
    ----------
    booster: xgb.Booster
        phase 1 booster to be saved as a checkpoint
    # feature_names: list
    #     booster's feature names
    string_tables: dict
        hash string tables for phase 1 FeatureEncoder
    model_seed: int
        model seed of phase 1 FeatureEncoder
    phase_index: int
        phase index; left for legacy purposes from time when phase 2 was also
        part of the trainer
    mean_item_count: float
        mean item count

    """

    mean_item_count = float(mean_item_count) if mean_item_count is not None else None

    append_metadata_to_booster(
        booster=booster, string_tables=string_tables, model_seed=model_seed,
        created_at=datetime.now().isoformat(), mean_item_count=mean_item_count)
    # get path for checkpoint
    checkpoint_save_path = CHECKPOINTS_PATH / f'phase{phase_index}.xgb'
    try:
        print(f'Attempting to save checkpoint for phase {phase_index}')
        booster.save_model(checkpoint_save_path)
    except xgb.core.XGBoostError as xgberr:
        print(f'Checkpoint save for phase {phase_index} failed with error:')
        print(str(xgberr))


# TODO this should probably be tested ?
def parse_max_checkpoint_age(max_checkpoint_age_string: str = None) -> timedelta:
    """
    Extract max_checkpoint_age from hyperparameters, verify value and unit and return timedelta object created with
    those values

    Returns
    -------
    timedelta
        timedelta created with extracted value and unit from hyperparamters
    """

    # assert that
    assert max_checkpoint_age_string is not None
    value_str, parsed_unit = re.sub(r'[\n\t\s]{1,}', ' ', max_checkpoint_age_string.strip()).split(' ')
    # parsed_unit is necessary because we allow hour and hours, etc. and timedelta() accepts only hours, minutes, ...
    assert parsed_unit in ALLOWED_MAX_CHECKPOINT_AGE_UNITS
    unit = ALLOWED_MAX_CHECKPOINT_AGE_UNITS[parsed_unit]
    # error on conversion will fail train job
    value = float(value_str)
    return timedelta(**{unit: value})


def get_feature_encoder_from_checkpoint(metadata):
    """
    Constructs FeatureEncoder from checkpoint's metadata

    Parameters
    ----------
    metadata: dict
        dictionary with checkpointed booster info

    Returns
    -------
    FeatureEncoder
        feature encoder created with provided model_seed
    """

    feature_names = metadata.get(FEATURE_NAMES_METADATA_KEY, None)
    assert feature_names is not None

    string_tables = metadata.get(STRING_TABLES_METADATA_KEY, None)
    assert string_tables is not None

    model_seed = int(metadata.get(MODEL_SEED_METADATA_KEY, None))
    assert model_seed is not None

    # feature_names: list, string_tables: dict, model_seed: int
    return FeatureEncoder(
        feature_names=feature_names, string_tables=string_tables, model_seed=model_seed)
