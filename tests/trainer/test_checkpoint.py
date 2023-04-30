from copy import deepcopy
import datetime
import importlib
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
from pytest import raises
import orjson
import os
import xgboost as xgb

from src.trainer.code import config
import src.trainer.code.checkpoint
from src.trainer.code.checkpoint import load_checkpoint, save_xgboost_checkpoint, \
    parse_max_checkpoint_age, use_checkpoint
from src.trainer.code.constants import DF_SCHEMA
from src.trainer.code.model_utils import FEATURE_NAMES_METADATA_KEY, \
    USER_DEFINED_METADATA_KEY, CREATED_AT_METADATA_KEY, MEAN_ITEM_COUNT_METADATA_KEY
from src.trainer.code.propensities import encode_partition
from src.trainer.code.string_encoder import StringEncoder


TEST_DATA_DIR = os.getenv('TEST_DATA_DIR', None)
assert TEST_DATA_DIR is not None

CHECKPOINT_DATA_CONTAINER_PATH = os.sep.join([TEST_DATA_DIR, 'checkpoint', 'sanity_check_propensities_metadata.json'])


def test_checkpoint_model():
    # remove old checkpoint before test
    checkpoint_path = os.sep.join([str(config.CHECKPOINTS_PATH), 'phase1.xgb'])
    delete_old_checkpoint = os.path.isfile(checkpoint_path)
    if delete_old_checkpoint:
        os.remove(checkpoint_path)

    with open(CHECKPOINT_DATA_CONTAINER_PATH, 'r') as f:
        checkpoint_test_dict = orjson.loads(f.read())

    df = pd.DataFrame(checkpoint_test_dict['records']).astype(DF_SCHEMA)
    feature_names = checkpoint_test_dict['feature_names']
    string_tables = checkpoint_test_dict['string_tables']
    model_seed = checkpoint_test_dict['model_seed']
    mean_item_count = checkpoint_test_dict['mean_item_count']

    # encode strings now
    string_encoder = \
        StringEncoder(string_tables=string_tables, model_seed=model_seed)
    features_with_encoded_strings = \
        [string_encoder.encode_strings(flat_features)
         for flat_features in encode_partition(df)]
    encoded_records_df = pd.DataFrame(features_with_encoded_strings, columns=feature_names + ['w', 'y'])

    # train booster
    w = encoded_records_df['w']
    y = encoded_records_df['y']
    X = encoded_records_df.drop(columns=['w', 'y'])

    dm = xgb.DMatrix(X, y, weight=w)

    params = {
        'objective': 'binary:logistic',
        'tree_method': 'hist',
        'verbosity': 1
    }

    # train booster
    booster = xgb.train(params, dm, num_boost_round=500)

    # predict and cache predictions
    test_dm = xgb.DMatrix(X)
    trained_model_predictions = booster.predict(test_dm)

    save_xgboost_checkpoint(
        booster=booster, string_tables=string_tables, model_seed=model_seed,
        phase_index=1, mean_item_count=mean_item_count)

    # load saved checkpoint -> call load_xgboost_checkpoint()
    loaded_booster, loaded_feature_encoder, loaded_mean_item_count = load_checkpoint()

    # recreate the dm with feature names from loaded booster
    loaded_checkpoint_metadata = orjson.loads(loaded_booster.attr(USER_DEFINED_METADATA_KEY))

    assert loaded_checkpoint_metadata[MEAN_ITEM_COUNT_METADATA_KEY] == loaded_mean_item_count
    assert mean_item_count == loaded_mean_item_count

    loaded_feature_names = loaded_checkpoint_metadata[FEATURE_NAMES_METADATA_KEY]

    np.testing.assert_array_equal(loaded_feature_names, booster.feature_names)
    np.testing.assert_array_equal(feature_names, booster.feature_names)

    loaded_dm = xgb.DMatrix(X.values, feature_names=loaded_feature_names)

    # predict with loaded checkpoint
    loaded_booster_predictions = loaded_booster.predict(loaded_dm)

    # assert predictions equal
    np.testing.assert_array_equal(trained_model_predictions, loaded_booster_predictions)

    # cleanup after test
    os.remove(checkpoint_path)


# TODO test parse_max_checkpoint_age

def test_parse_max_checkpoint_age():

    checkpoint_ages = [
        ('1 second', datetime.timedelta(**{'seconds': 1})),
        ('123 seconds', datetime.timedelta(**{'seconds': 123})),
        ('1 minute', datetime.timedelta(**{'minutes': 1})),
        ('123 minutes', datetime.timedelta(**{'minutes': 123})),
        ('1 hour', datetime.timedelta(**{'hours': 1})),
        ('123 hours', datetime.timedelta(**{'hours': 123})),
        ('1 day', datetime.timedelta(**{'days': 1})),
        ('123 days', datetime.timedelta(**{'days': 123}))]

    for checkpoint_age_str, expected_checkpoint_age in checkpoint_ages:
        assert parse_max_checkpoint_age(checkpoint_age_str) == expected_checkpoint_age


def test_parse_max_checkpoint_age_raises_for_none():
    with raises(AssertionError) as aerr:
        parse_max_checkpoint_age(None)


def test_parse_max_checkpoint_age_raises_for_invalid_unit():

    invalid_unit_strings = [
        '1 nanosecond',
        '1 microsecond',
        '2 milliseconds',
        '3 weeks',
        '1 month',
        '3 months'
    ]

    for bad_unit_str in invalid_unit_strings:
        with raises(AssertionError) as aerr:
            parse_max_checkpoint_age(bad_unit_str)


def test_parse_max_checkpoint_age_raises_for_invalid_value():
    with raises(ValueError) as verr:
        parse_max_checkpoint_age('abc second')


# TODO test use_checkpoint
def test_use_checkpoint_true():
    created_at = datetime.datetime.now() - datetime.timedelta(**{'hours': 23})
    assert use_checkpoint(created_at)


def test_use_checkpoint_false():
    created_at = datetime.datetime.now() - datetime.timedelta(**{'hours': 26})
    assert not use_checkpoint(created_at)


def test_use_checkpoint_raises_for_bad_created_at_type():
    bad_created_at_types = [
        1, 1.23, '1.23', True, [1, 2, 3], (3, 2, 1), {1, 2, 3}, {'a': 1, 'b': 2},
        np.array([1, 2, 3])]
    for bad_type_created_at in bad_created_at_types:
        with raises(AssertionError) as aerr:
            use_checkpoint(bad_type_created_at)


def test_use_checkpoint_raises_for_none_created_at():
    with raises(AssertionError) as aerr:
        use_checkpoint(None)


# TODO test that checkpoint is loaded / not loaded for a given checkpoint age
#  hyperparam

def _prepare_checkpoint_with_desired_age(created_at):

    # get tested booster
    dummy_checkpoint_path = os.getenv('TEST_MODEL_METADATA_BOOSTER_PATH', None)
    assert dummy_checkpoint_path is not None
    b = xgb.Booster()
    b.load_model(dummy_checkpoint_path)

    # modify created_at
    b_meta = orjson.loads(b.attr(USER_DEFINED_METADATA_KEY))

    b_meta[CREATED_AT_METADATA_KEY] = created_at
    b_meta[MEAN_ITEM_COUNT_METADATA_KEY] = 100.0

    b.set_attr(**{USER_DEFINED_METADATA_KEY: orjson.dumps(b_meta).decode('utf-8')})

    # dump booster to a temp file
    test_data_path = os.getenv('TEST_DATA_DIR', None)
    assert test_data_path is not None

    checkpoint_test_dir = Path(test_data_path) / 'checkpoint_booster'
    checkpoint_test_dir.mkdir(exist_ok=True, parents=True)

    checkpoint_booster_path = checkpoint_test_dir / 'phase1.xgb'

    b.save_model(checkpoint_booster_path)
    return checkpoint_test_dir


def _get_max_checkpoint_ages_for_valid_units(add_0_seconds_test_case: bool = False):
    return \
        {f'{value} {unit if value != 1 else unit[:-1]}': datetime.timedelta(**{unit: value})
         for value, unit in zip(
            ([0] if add_0_seconds_test_case else []) + [1, 30, 1, 30, 1, 24, 1, 2],
            (['seconds'] if add_0_seconds_test_case else []) + ['seconds', 'seconds', 'minutes', 'minutes', 'hours', 'hours', 'days', 'days'])}


def test_checkpoint_younger_than_max_checkpoint_age_loads():

    max_checkpoint_ages = _get_max_checkpoint_ages_for_valid_units()

    for mca_string, mca in max_checkpoint_ages.items():
        now = datetime.datetime.now()

        checkpoint_save_dir = _prepare_checkpoint_with_desired_age(created_at=(now - mca * 0.75).isoformat())

        # expect checkpoint to be saved under phase1.xgb
        config.CHECKPOINTS_PATH = Path(checkpoint_save_dir)
        src.trainer.code.checkpoint.CHECKPOINTS_PATH = Path(checkpoint_save_dir)

        config.MAX_CHECKPOINT_AGE_STRING = mca_string
        src.trainer.code.checkpoint.MAX_CHECKPOINT_AGE_STRING = mca_string

        load_checkpoint_result = load_checkpoint()
        assert load_checkpoint_result is not None
        checkpoint_booster, checkpoint_feature_encoder, mean_item_count = load_checkpoint_result

        assert isinstance(mean_item_count, float)
        # apparently if the import path changes the isinstance() result will be affected
        assert isinstance(checkpoint_feature_encoder, src.trainer.code.checkpoint.FeatureEncoder)
        assert isinstance(checkpoint_booster, xgb.Booster)

        # make sure created at stored in booster's metadata is valid
        created_at = orjson.loads(checkpoint_booster.attr(USER_DEFINED_METADATA_KEY)).get(CREATED_AT_METADATA_KEY, None)
        assert created_at is not None
        created_at_dt = datetime.datetime.fromisoformat(created_at)
        assert now - created_at_dt <= mca
        # check that is has metadata
        shutil.rmtree(checkpoint_save_dir)


def test_checkpoint_older_than_max_checkpoint_age_does_not_load():
    max_checkpoint_ages = _get_max_checkpoint_ages_for_valid_units(add_0_seconds_test_case=True)

    for mca_string, mca in max_checkpoint_ages.items():
        now = datetime.datetime.now()
        checkpoint_save_dir = _prepare_checkpoint_with_desired_age(
            created_at=(now - mca * 1.25).isoformat())
        # expect checkpoint to be saved under phase1.xgb
        config.CHECKPOINTS_PATH = Path(checkpoint_save_dir)
        src.trainer.code.checkpoint.CHECKPOINTS_PATH = Path(checkpoint_save_dir)

        config.MAX_CHECKPOINT_AGE_STRING = mca_string
        src.trainer.code.checkpoint.MAX_CHECKPOINT_AGE_STRING = mca_string

        load_checkpoint_result = load_checkpoint()
        assert load_checkpoint_result is None

        shutil.rmtree(checkpoint_save_dir)


def test_zero_seconds_max_checkpoint_age():
    dummy_created_at = datetime.datetime.now()
    checkpoint_save_dir = \
        _prepare_checkpoint_with_desired_age(created_at=dummy_created_at.isoformat())
    config.CHECKPOINTS_PATH = Path(checkpoint_save_dir)
    src.trainer.code.checkpoint.CHECKPOINTS_PATH = Path(checkpoint_save_dir)

    config.MAX_CHECKPOINT_AGE_STRING = '0 seconds'
    src.trainer.code.checkpoint.MAX_CHECKPOINT_AGE_STRING = '0 seconds'

    load_checkpoint_result = load_checkpoint()
    assert load_checkpoint_result is None

    shutil.rmtree(checkpoint_save_dir)


def test_load_checkpoint_with_missing_max_checkpoint_age_hyperparam():
    # cache old hyperparams
    previous_hyperparams = deepcopy(config.HYPERPARAMETERS)
    # modify hyperparams -> remove checkpoint entry
    modified_hyperparams = deepcopy(config.HYPERPARAMETERS)
    del modified_hyperparams[config.MAX_CHECKPOINT_AGE_HYPERPARAMS_KEY]

    with open(config.HYPERPARAMETER_PATH, 'w') as f:
        f.write(orjson.dumps(modified_hyperparams).decode())

    importlib.reload(config)

    # use importlib.reload(config_1) to reload config
    assert config.MAX_CHECKPOINT_AGE_STRING not in config.HYPERPARAMETERS
    assert config.MAX_CHECKPOINT_AGE_STRING == '0 seconds'

    dummy_created_at = datetime.datetime.now()
    checkpoint_save_dir = \
        _prepare_checkpoint_with_desired_age(
            created_at=dummy_created_at.isoformat())

    config.CHECKPOINTS_PATH = Path(checkpoint_save_dir)
    src.trainer.code.checkpoint.CHECKPOINTS_PATH = Path(checkpoint_save_dir)

    load_checkpoint_result = load_checkpoint()
    assert load_checkpoint_result is None

    shutil.rmtree(checkpoint_save_dir)

    # restore old hyperparams
    with open(config.HYPERPARAMETER_PATH, 'w') as f:
        f.write(orjson.dumps(previous_hyperparams).decode())

    pass
