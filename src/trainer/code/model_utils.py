from datetime import datetime

import coremltools as ct
import dask
from dask_ml.model_selection import train_test_split
import numpy as np
# TODO import orjson as json -> this is misleading -> json.dump does not require decoding afterwards
# import orjson as json
import orjson
import pandas as pd
import xgboost as xgb

from config import VERSION, TEST_SPLIT, MODEL_NAME
import constants
# import improveai
from utils import trim_memory

MODEL_NAME_METADATA_KEY = 'ai.improve.model'
FEATURE_NAMES_METADATA_KEY = 'ai.improve.features'
STRING_TABLES_METADATA_KEY = 'ai.improve.string_tables'
MODEL_SEED_METADATA_KEY = 'ai.improve.seed'
CREATED_AT_METADATA_KEY = 'ai.improve.created_at'
VERSION_METADATA_KEY = 'ai.improve.version'

MEAN_ITEM_COUNT_METADATA_KEY = 'ai.improve.mean_item_count'

USER_DEFINED_METADATA_KEY = 'user_defined_metadata'
MLMODEL_REGRESSOR_MODE = 'regressor'

# suppress scientific notation for pandas
pd.options.display.float_format = '{:g}'.format


def train_xgb(client, wrapped_features_bag: list, feature_names, params, num_boost_round=200, early_stopping_rounds=20):
    
    # remove from its wrapping to free the reference
    features_bag = wrapped_features_bag.pop()

    assert constants.TARGET_FEATURE_KEY not in feature_names
    assert constants.WEIGHT_FEATURE_KEY not in feature_names
    columns = [constants.TARGET_FEATURE_KEY, constants.WEIGHT_FEATURE_KEY] + feature_names

    # create the dataframe
    meta = pd.DataFrame(columns=columns, dtype=np.float32)
    df = features_bag.to_dataframe(meta=meta)

    # garbage collect
    del features_bag
    client.run(trim_memory)

    # persist the dataframe since it will be used multiple times
    print('persisting dataframe')
    df = client.persist(df)

    # split the dataframe
    y = df[constants.TARGET_FEATURE_KEY]
    w = df[constants.WEIGHT_FEATURE_KEY]
    X = df[df.columns.difference([constants.TARGET_FEATURE_KEY, constants.WEIGHT_FEATURE_KEY])]

    print(f'y {single_line_describe(y)}')
    print(f'w {single_line_describe(w)}')

    # garbage collect
    del df
    client.run(trim_memory)

    X_train, X_test, y_train, y_test, w_train, w_test = train_test_split(X, y, w, test_size=TEST_SPLIT)
    
    # garbage collect
    del y
    del w
    del X
    client.run(trim_memory)
    
    print('persisting dataframes')
    X_train, X_test, y_train, y_test, w_train, w_test = dask.persist(X_train, X_test, y_train, y_test, w_train, w_test)

    print('create dtrain')
    dtrain = xgb.dask.DaskDMatrix(client, X_train, y_train, weight=w_train)
    
    # garbage collect
    del X_train
    del y_train
    del w_train
    client.run(trim_memory)
    
    print('create dtest')
    dtest = xgb.dask.DaskDMatrix(client, X_test, y_test, weight=w_test)

    # garbage collect
    del X_test
    del y_test
    del w_test
    client.run(trim_memory)

    print('start training')
    output = xgb.dask.train(client,
                            params,
                            dtrain,
                            num_boost_round=num_boost_round,
                            early_stopping_rounds=early_stopping_rounds,
                            evals=[(dtest, 'test')])

    del dtrain, dtest
    client.run(trim_memory)

    return output['booster']  # booster is the trained model
    
    # stats = ddf[REWARD_KEY].describe().compute()

    # mean = stats.loc['mean']
    # std = stats.loc['std']

    # count   10
    # mean     0
    # std      0
    # min      0
    # 25%      0
    # 50%      0
    # 75%      0
    # max      0

    
def single_line_describe(df):
    desc = str(df.describe().compute())
    # sorry, no awards for legibility here. chops off the last item, which is dtype
    # count: 152, mean: 0.874557, std: 1.95162, min: 0.0386769, 25%: 0.0400877, 50%: 0.0787821, 75%: 0.61204, max: 9.65612
    return ': '.join(','.join(desc.split('\n')[:-1]).split()).replace(',', ', ')


def _check_user_defined_metadata(user_defined_metadata: dict):
    """
    Checks if provided metadata contains any Nones. If it does raises an AssertionError

    Parameters
    ----------
    user_defined_metadata: dict
        user defined metadata to be checked

    Raises
    -------
    AssertionError
        if any of the values in the user defined metadata is None

    """
    assert user_defined_metadata is not None and isinstance(user_defined_metadata, dict) \
           and len(user_defined_metadata) > 0, \
           'User defined metadata must not be None - it must be of a dict type.'

    # below is the list of currently required keys in Improve AI model metadata
    required_keys = [MODEL_NAME_METADATA_KEY, STRING_TABLES_METADATA_KEY,
                     MODEL_SEED_METADATA_KEY, CREATED_AT_METADATA_KEY, VERSION_METADATA_KEY]

    # below is the list of expected types for required keys
    required_types_for_keys = [str, dict, int, str, str]
    # for each key check that:
    # - it is present in the metadata and is not set to None
    # - value is of a desired type
    # - if meta info is `created_at` check that datetime.fromisoformat() is able to parse it properly
    for metadata_key, required_type in zip(required_keys, required_types_for_keys):
        metadata_entry = user_defined_metadata.get(metadata_key, None)
        assert metadata_entry is not None and isinstance(metadata_entry, required_type), \
            f'Bad metadata value: {metadata_entry} stored under {metadata_key} key'
        if metadata_key == CREATED_AT_METADATA_KEY:
            datetime.fromisoformat(metadata_entry)

    # if feature names are present in metadata (xgb booster case) make sure that
    # they are a non-empty list
    if FEATURE_NAMES_METADATA_KEY in user_defined_metadata:
        feature_names = user_defined_metadata[FEATURE_NAMES_METADATA_KEY]
        assert isinstance(feature_names, list) and len(feature_names) > 0, \
            f'Bad metadata value: {feature_names} stored under {FEATURE_NAMES_METADATA_KEY} key'


# TODO test this
def append_metadata_to_mlmodel(mlmodel, string_tables, model_seed, created_at):
    """
    Appends metadata needed by the Improve AI Scorer/Ranker

    Parameters
    ----------
    mlmodel: coremltools.models.MLModel
        mlmodel to be appended with metadata
    string_tables: dict
        a dict with target-value encoding for strings
    model_seed: int
        model seed for the feature encoder for this model
    created_at: str
        a string representing the date and time the model was created

    """

    input_user_defined_metadata = dict(zip(
        [MODEL_NAME_METADATA_KEY, STRING_TABLES_METADATA_KEY, MODEL_SEED_METADATA_KEY,
         CREATED_AT_METADATA_KEY, VERSION_METADATA_KEY],
        [MODEL_NAME, string_tables, model_seed, created_at, VERSION]))

    _check_user_defined_metadata(input_user_defined_metadata)

    input_user_defined_metadata[STRING_TABLES_METADATA_KEY] = orjson.dumps(string_tables).decode('utf-8')
    input_user_defined_metadata[MODEL_SEED_METADATA_KEY] = str(model_seed)

    mlmodel.user_defined_metadata.update(input_user_defined_metadata)


# TODO test this
def append_metadata_to_booster(
        booster, string_tables, model_seed, created_at, mean_item_count: int = None):
    """
    Appends provided metadata to the booster under `USER_DEFINED_METADATA_KEY` attribute.
    Dict with metadata must be dumped to string before it can be appended to booster

    Parameters
    ----------
    booster: xgboost.Booster
        a booster wo which metadata will be appended
    # feature_names: list
    #     feature names for a given booster
    string_tables: dict
        a dict with target-value encoding for strings
    model_seed: int
        model seed for the feature encoder for this model
    created_at: str
        date and time of this model creation
    mean_item_count: int
        mean item / candidates count per decision

    """

    assert booster.feature_names is not None and len(booster.feature_names) > 0

    booster_metadata = {
        MODEL_NAME_METADATA_KEY: MODEL_NAME,
        FEATURE_NAMES_METADATA_KEY: booster.feature_names,
        STRING_TABLES_METADATA_KEY: string_tables,
        MODEL_SEED_METADATA_KEY: model_seed,
        CREATED_AT_METADATA_KEY: created_at,
        VERSION_METADATA_KEY: VERSION
    }

    # assert that all values are valid
    _check_user_defined_metadata(booster_metadata)

    if mean_item_count is not None:
        booster_metadata[MEAN_ITEM_COUNT_METADATA_KEY] = mean_item_count

    booster_metadata_json = orjson.dumps(booster_metadata).decode('utf-8')

    booster.set_attr(**{USER_DEFINED_METADATA_KEY:  booster_metadata_json})


def _assert_feature_names_identical_in_booster_and_mlmodel(booster: xgb.Booster, mlmodel: ct.models.MLModel):
    """
    Make sure that feature names are identical in booster and mlmodel

    Parameters
    ----------
    booster: xgb.Booster
        DecisionModel's booster
    mlmodel: ct.models.MLModel
        a MLModel converted from provided booster

    """

    # check that feature names are in an identical order
    # first way to extract feature names from mlmodel
    np.testing.assert_array_equal(list(booster.feature_names), [fn.name for fn in mlmodel.input_description._fd_spec])
    # second way to extract feature names from mlmodel - make sure they are same everywhere
    np.testing.assert_array_equal(list(booster.feature_names), [fn.name for fn in mlmodel.get_spec().description.input])
    # load feature names from booster's metadata
    booster_metadata_feature_names = orjson.loads(booster.attr(USER_DEFINED_METADATA_KEY))[FEATURE_NAMES_METADATA_KEY]
    # make sure booster feature names attribute has identical value as metadata
    np.testing.assert_array_equal(list(booster.feature_names), booster_metadata_feature_names)
    # make sure feature names stored in mlmodel are identical with those extracted from bosoter's metadata
    np.testing.assert_array_equal(booster_metadata_feature_names, [fn.name for fn in mlmodel.get_spec().description.input])


def transform_model(booster: xgb.Booster, string_tables: dict, model_seed: int) -> tuple:
    # feature_names = booster.feature_names
    assert booster.feature_names is not None, 'Booster must have feature names'

    unexpected_feature_names = [constants.TARGET_FEATURE_KEY, constants.WEIGHT_FEATURE_KEY]
    assert all([fn not in unexpected_feature_names for fn in booster.feature_names]), \
        f'Booster feature names contain unexpected feature names: {unexpected_feature_names}'

    created_at = datetime.now().isoformat()

    # Convert model
    mlmodel = ct.converters.xgboost.convert(
        booster, mode=MLMODEL_REGRESSOR_MODE, feature_names=booster.feature_names, force_32bit_float=True)

    # append Improve AI metadata to mlmodel
    append_metadata_to_mlmodel(
        mlmodel=mlmodel, string_tables=string_tables, model_seed=model_seed, created_at=created_at)

    # for decision model `mean_item_count` should be None (only propensity model is checkpointed)
    append_metadata_to_booster(
        booster=booster, string_tables=string_tables, model_seed=model_seed,
        created_at=created_at, mean_item_count=None)

    _assert_feature_names_identical_in_booster_and_mlmodel(booster, mlmodel)

    return booster, mlmodel
