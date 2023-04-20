from copy import deepcopy
from datetime import datetime
import os

import coremltools as ct
import numpy as np
import orjson
from pytest import fixture, raises
import xgboost as xgb

from src.trainer.code.config import MODEL_NAME, VERSION
from src.trainer.code.feature_encoder import CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY
from src.trainer.code.model_utils import _check_user_defined_metadata, \
    append_metadata_to_mlmodel, append_metadata_to_booster, transform_model, \
    _assert_feature_names_identical_in_booster_and_mlmodel
from src.trainer.code.model_utils import MODEL_NAME_METADATA_KEY, FEATURE_NAMES_METADATA_KEY, \
    STRING_TABLES_METADATA_KEY, MODEL_SEED_METADATA_KEY, CREATED_AT_METADATA_KEY, \
    MEAN_ITEM_COUNT_METADATA_KEY, VERSION_METADATA_KEY, USER_DEFINED_METADATA_KEY, \
    MLMODEL_REGRESSOR_MODE

# TODO test:
#  - appending data to mlmodel +
#  - _check_user_defined_metadata +
#  - appending data to booster +
#  - transform model

# mlmodel will be needed to test metadata appending
global mlmodel
mlmodel = None

global booster
booster = None


@fixture(autouse=True)
def prep_env():
    booster_path = os.getenv('TEST_MODEL_METADATA_BOOSTER_PATH', None)
    assert booster_path is not None, 'TEST_MODEL_METADATA_BOOSTER_PATH is not set'
    # fully load sample v8 booster
    global booster
    booster = xgb.Booster()
    booster.load_model(booster_path)
    booster_metadata = orjson.loads(booster.attr(USER_DEFINED_METADATA_KEY))
    feature_names = booster_metadata[FEATURE_NAMES_METADATA_KEY]
    # delete metadata attribute
    booster.set_attr(**{USER_DEFINED_METADATA_KEY: None})
    # make sure user defined metadata is deleted
    assert booster.attr(USER_DEFINED_METADATA_KEY) is None
    # convert to mlmodel
    global mlmodel
    mlmodel = ct.converters.xgboost.convert(
        booster, mode=MLMODEL_REGRESSOR_MODE, feature_names=feature_names, force_32bit_float=True)
    # now both booster and mlmodel are ready to be tested


def test__check_user_defined_metadata_raises_for_none():
    with raises(AssertionError) as aerr:
        _check_user_defined_metadata(None)


def test__check_user_defined_metadata_raises_for_bad_type():

    for bad_type_metadata in [[1, 2, 3], (1, 2, 3), {1, 2, 3}, np.array([]),
                              np.array([1, 2, 3]), '1, 2, 3', 1, 1.0, True]:
        with raises(AssertionError) as aerr:
            _check_user_defined_metadata(bad_type_metadata)


def test__check_user_defined_metadata_raises_for_empty_dict():
    with raises(AssertionError) as aerr:
        _check_user_defined_metadata({})


def test__check_user_defined_metadata_raises_for_missing_metadata_keys():
    m = {MODEL_NAME_METADATA_KEY: 'dummy-model',
         FEATURE_NAMES_METADATA_KEY: ['context', 'item']}

    with raises(AssertionError) as aerr:
        _check_user_defined_metadata(m)


def test__check_user_defined_metadata_raises_for_none_values():
    m = {MODEL_NAME_METADATA_KEY: 'dummy-model',
         FEATURE_NAMES_METADATA_KEY: ['context', 'item']}

    with raises(AssertionError) as aerr:
        _check_user_defined_metadata(m)


def test__check_user_defined_metadata_raises_for_bad_value_type():
    m = {
        MODEL_NAME_METADATA_KEY: 'dummy-model',
        STRING_TABLES_METADATA_KEY: 'abc',
        MODEL_SEED_METADATA_KEY: 13,
        CREATED_AT_METADATA_KEY: '2020-01-01T00:00:00',
        VERSION_METADATA_KEY: '8.0.0'
    }
    with raises(AssertionError) as aerr:
        _check_user_defined_metadata(m)


def test__check_user_defined_metadata_raises_for_bad_date_string():
    m = {
        MODEL_NAME_METADATA_KEY: 'dummy-model',
        STRING_TABLES_METADATA_KEY: {},
        MODEL_SEED_METADATA_KEY: 13,
        CREATED_AT_METADATA_KEY: 'abc123',
        VERSION_METADATA_KEY: '8.0.0'
    }
    with raises(ValueError) as verr:
        _check_user_defined_metadata(m)


def test__check_user_defined_metadata_01():
    m = {
        MODEL_NAME_METADATA_KEY: 'dummy-model',
        FEATURE_NAMES_METADATA_KEY: ['a', 'b', 'c'],
        STRING_TABLES_METADATA_KEY: {},
        MODEL_SEED_METADATA_KEY: 13,
        CREATED_AT_METADATA_KEY: '2020-01-01T00:00:00',
        VERSION_METADATA_KEY: '8.0.0'
    }
    _check_user_defined_metadata(m)


def test__check_user_defined_metadata_02():
    m = {
        MODEL_NAME_METADATA_KEY: 'dummy-model',
        STRING_TABLES_METADATA_KEY: {},
        MODEL_SEED_METADATA_KEY: 13,
        CREATED_AT_METADATA_KEY: '2020-01-01T00:00:00',
        VERSION_METADATA_KEY: '8.0.0'
    }
    _check_user_defined_metadata(m)


def test_append_metadata_to_mlmodel():
    global mlmodel
    tested_string_tables = {"item": [3, 2, 1]}
    tested_model_seed = 13
    tested_created_at = '2020-01-01T00:00:00'
    # mlmodel, string_tables, model_seed, created_at
    append_metadata_to_mlmodel(
        mlmodel=mlmodel, string_tables=tested_string_tables,
        model_seed=tested_model_seed, created_at=tested_created_at)
    # extract metadata from modified mlmodel and check if it is identical with

    expected_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        STRING_TABLES_METADATA_KEY: orjson.dumps(deepcopy(tested_string_tables)).decode('utf-8'),
        MODEL_SEED_METADATA_KEY: str(tested_model_seed),
        CREATED_AT_METADATA_KEY: tested_created_at,
        VERSION_METADATA_KEY: VERSION
    }

    appended_metadata = {}
    for mk in expected_metadata.keys():
        appended_metadata[mk] = mlmodel.user_defined_metadata[mk]
    assert appended_metadata == expected_metadata


def test_append_metadata_to_booster_no_mean_item_count():
    global booster
    feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY]
    booster.feature_names = feature_names
    tested_string_tables = {"item": [3, 2, 1]}
    tested_model_seed = 13
    tested_created_at = '2020-01-01T00:00:00'
    # booster, feature_names, string_tables, model_seed, created_at
    append_metadata_to_booster(
        booster=booster, string_tables=tested_string_tables, model_seed=tested_model_seed,
        created_at=tested_created_at)
    # extract metadata from modified mlmodel and check if it is identical with

    expected_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        FEATURE_NAMES_METADATA_KEY: deepcopy(feature_names),
        STRING_TABLES_METADATA_KEY: deepcopy(tested_string_tables),
        MODEL_SEED_METADATA_KEY: tested_model_seed,
        CREATED_AT_METADATA_KEY: tested_created_at,
        VERSION_METADATA_KEY: VERSION
    }

    appended_metadata = orjson.loads(booster.attr(USER_DEFINED_METADATA_KEY))
    assert appended_metadata == expected_metadata


# TODO test mean item count
def test_append_metadata_to_booster_valid_mean_item_count():
    global booster
    feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY]
    booster.feature_names = feature_names
    tested_string_tables = {"item": [3, 2, 1]}
    tested_model_seed = 13
    tested_created_at = '2020-01-01T00:00:00'
    mean_item_count = 100.1
    # booster, feature_names, string_tables, model_seed, created_at
    append_metadata_to_booster(
        booster=booster, string_tables=tested_string_tables, model_seed=tested_model_seed,
        created_at=tested_created_at, mean_item_count=mean_item_count)
    # extract metadata from modified mlmodel and check if it is identical with

    expected_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        FEATURE_NAMES_METADATA_KEY: deepcopy(feature_names),
        STRING_TABLES_METADATA_KEY: deepcopy(tested_string_tables),
        MODEL_SEED_METADATA_KEY: tested_model_seed,
        CREATED_AT_METADATA_KEY: tested_created_at,
        VERSION_METADATA_KEY: VERSION,
        MEAN_ITEM_COUNT_METADATA_KEY: mean_item_count
    }

    appended_metadata = orjson.loads(booster.attr(USER_DEFINED_METADATA_KEY))
    assert appended_metadata == expected_metadata


# TODO test mean item count
def test_append_metadata_to_booster_valid_none_item_count():
    global booster
    feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY]
    booster.feature_names = feature_names
    tested_string_tables = {"item": [3, 2, 1]}
    tested_model_seed = 13
    tested_created_at = '2020-01-01T00:00:00'
    mean_item_count = None
    # booster, feature_names, string_tables, model_seed, created_at
    append_metadata_to_booster(
        booster=booster, string_tables=tested_string_tables, model_seed=tested_model_seed,
        created_at=tested_created_at, mean_item_count=mean_item_count)
    # extract metadata from modified mlmodel and check if it is identical with

    expected_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        FEATURE_NAMES_METADATA_KEY: deepcopy(feature_names),
        STRING_TABLES_METADATA_KEY: deepcopy(tested_string_tables),
        MODEL_SEED_METADATA_KEY: tested_model_seed,
        CREATED_AT_METADATA_KEY: tested_created_at,
        VERSION_METADATA_KEY: VERSION,
    }

    appended_metadata = orjson.loads(booster.attr(USER_DEFINED_METADATA_KEY))
    assert appended_metadata == expected_metadata


def test_transform_model():
    # booster: xgb.Booster, feature_names: list, string_tables: dict, model_seed: int
    feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY]
    string_tables = {"context": [3, 2, 1], "item": [12, 8, 4]}
    model_seed = 123
    just_before_transform = datetime.now()

    global booster
    booster.feature_names = feature_names
    appended_booster, appended_mlmodel = \
        transform_model(booster=booster, string_tables=string_tables, model_seed=model_seed)

    just_after_transform = datetime.now()

    # make sure booster's metadata is valid
    expected_booster_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        FEATURE_NAMES_METADATA_KEY: deepcopy(feature_names),
        STRING_TABLES_METADATA_KEY: deepcopy(string_tables),
        MODEL_SEED_METADATA_KEY: model_seed,
        # CREATED_AT_METADATA_KEY: None,  # -> this will be deleted and tested to be between 'before' and 'after' values
        VERSION_METADATA_KEY: VERSION
    }

    assert isinstance(appended_booster, xgb.Booster)
    appended_metadata = orjson.loads(appended_booster.attr(USER_DEFINED_METADATA_KEY))
    # make sure created_at is valid
    assert appended_metadata[CREATED_AT_METADATA_KEY] is not None
    created_at = datetime.fromisoformat(appended_metadata[CREATED_AT_METADATA_KEY])
    assert just_before_transform < created_at < just_after_transform

    # remove created_at key
    del appended_metadata[CREATED_AT_METADATA_KEY]

    assert appended_metadata == expected_booster_metadata

    assert isinstance(appended_mlmodel, ct.models.MLModel)

    assert appended_mlmodel.user_defined_metadata[CREATED_AT_METADATA_KEY] is not None
    created_at = datetime.fromisoformat(
        appended_mlmodel.user_defined_metadata[CREATED_AT_METADATA_KEY])
    assert just_before_transform < created_at < just_after_transform

    expected_mlmodel_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        STRING_TABLES_METADATA_KEY: orjson.dumps(deepcopy(string_tables)).decode('utf-8'),
        MODEL_SEED_METADATA_KEY: str(model_seed),
        # CREATED_AT_METADATA_KEY: None,  # -> this will be deleted and tested to be between 'before' and 'after' values
        VERSION_METADATA_KEY: VERSION
    }

    # on purpose skipping create_at
    obligatory_mlmodel_metadata_keys = [
        MODEL_NAME_METADATA_KEY, STRING_TABLES_METADATA_KEY, MODEL_SEED_METADATA_KEY, VERSION_METADATA_KEY]
    for mk in obligatory_mlmodel_metadata_keys:
        expected_value = expected_mlmodel_metadata[mk]
        appended_value = appended_mlmodel.user_defined_metadata[mk]
        assert appended_value == expected_value


def test__assert_feature_names_identical_in_booster_and_mlmodel_passes_for_identical_feature_names():
    global booster

    feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY]
    booster.feature_names = feature_names
    booster_metadata = {FEATURE_NAMES_METADATA_KEY: feature_names}
    # only a portion of metadata is needed for this test
    booster.set_attr(**{USER_DEFINED_METADATA_KEY: orjson.dumps(booster_metadata).decode('utf-8')})

    mlmodel = ct.converters.xgboost.convert(
        booster, mode=MLMODEL_REGRESSOR_MODE, feature_names=booster.feature_names, force_32bit_float=True)

    _assert_feature_names_identical_in_booster_and_mlmodel(booster=booster, mlmodel=mlmodel)


def test__assert_feature_names_identical_in_booster_and_mlmodel_raises_for_differing_feature_names():
    global booster

    feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY]
    booster.feature_names = feature_names

    mlmodel = ct.converters.xgboost.convert(
        booster, mode=MLMODEL_REGRESSOR_MODE,
        feature_names=booster.feature_names, force_32bit_float=True)

    booster.feature_names = [CONTEXT_FEATURE_KEY, ITEM_FEATURE_KEY, 'some_other_feature']

    with raises(AssertionError) as aerr:
        _assert_feature_names_identical_in_booster_and_mlmodel(booster=booster, mlmodel=mlmodel)
