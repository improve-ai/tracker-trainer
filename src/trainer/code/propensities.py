import itertools
from ksuid import Ksuid
import numpy as np
import pandas as pd
from xgboost import DMatrix

from checkpoint import load_checkpoint, save_xgboost_checkpoint
from config import CLIP_MIN_PROPENSITY, REWARDED_DECISIONS_PATH, MAX_DECISION_RECORDS, EXPLORE
from exploration import PARQUET_FILE_SAMPLE
from constants import DECISION_ID_KEY, ITEM_KEY, CONTEXT_KEY, SAMPLE_KEY, COUNT_KEY, TIMESTAMP_FEATURE_KEY, TARGET_FEATURE_KEY, WEIGHT_FEATURE_KEY
import model_utils
from parquet_io import DataFrameLoader
from feature_selection import select_features
from feature_flattener import flatten_item, flatten_context
from string_encoder import encode_strings
from feature_encoder import FeatureEncoder
from utils import random_model_seed

class PropensityModel:
    
    
    def __init__(self, booster, feature_encoder, mean_item_count):
        self.booster = booster
        self.feature_encoder = feature_encoder
        assert mean_item_count >= 1 # records with 'count' < 1 are invalid
        self.mean_item_count = mean_item_count
    

    def selected_features(self):
        return list(filter(lambda x: x != TIMESTAMP_FEATURE_KEY, self.booster.feature_names))


    def normalized_inverse_propensity_weights(self, df):
        row_count = df.shape[0]

        A = np.full([row_count, len(self.booster.feature_names)], np.nan) # create matrix filled with nan
        for row_index, decision_id, item, context in zip(range(row_count), df[DECISION_ID_KEY], df[ITEM_KEY], df[CONTEXT_KEY]):
            row = A[row_index]
            # # TODO enable after debug complete
            extra_features = { TIMESTAMP_FEATURE_KEY: Ksuid.from_base62(decision_id).datetime.timestamp() }
            self.feature_encoder.encode_feature_vector(item=item, context=context, extra_features=extra_features, into=row)

        propensities = self.booster.predict(DMatrix(A, feature_names=self.booster.feature_names))

        for item, context, propensity in zip(df[ITEM_KEY][:5], df[CONTEXT_KEY][:5], propensities[:5]):
            print(f'propensity: {propensity} item: {item} context: {context}')
        
        # take the inverse of the propensity, then divide by the mean variant count to normalize the mean propensity weight toward 1
        return list(map(lambda p: (1 / max(p, CLIP_MIN_PROPENSITY)) / self.mean_item_count, propensities))


    def save(self, string_tables, model_seed):
        # make sure feature names stored in the booster are identical with the
        # ones from feature encoder
        np.testing.assert_array_equal(
            list(self.booster.feature_names), list(self.feature_encoder.feature_indexes.keys()))
        # save checkpoint
        save_xgboost_checkpoint(
            booster=self.booster, string_tables=string_tables, model_seed=model_seed, phase_index=1,
            mean_item_count=self.mean_item_count)

        

def train(client) -> PropensityModel:
    
    print('starting phase 1')
    checkpoint = load_checkpoint()
    if checkpoint is not None:
        print('using checkpoint model for phase 1')
        return PropensityModel(*checkpoint) # TODO sorry a bit ugly, need to resolve circle dependecy in checkpoint_utils

    # Set both min_rows and max_rows to MAX_DECISION_RECORDS so that min_rows overrides the sample and loads as many records as possible in the case where there are
    # less than MAX_DECISION_RECORDS x ~63% records available when exploring. We use the ~63% sample here so that in the case of greater than MAX_DECISION_RECORDS x ~63% records available
    # the propensity records will cover a similar span of time as the decision records that we'll train on for the final decision model.
    loader = DataFrameLoader(client, parquet_path=REWARDED_DECISIONS_PATH, min_rows=MAX_DECISION_RECORDS, max_rows=MAX_DECISION_RECORDS, sample=PARQUET_FILE_SAMPLE)
    ddf = loader.load(columns=[DECISION_ID_KEY, ITEM_KEY, CONTEXT_KEY, SAMPLE_KEY, COUNT_KEY])

    print(f'Loaded records count: {ddf.shape[0].compute()}')

    # ddf.describe() is probably the most efficient way to compute multiple statistics at once
    # stats = ddf[COUNT_KEY].describe().compute()

    # mean_item_count = stats.loc['mean']
    mean_item_count = ddf[COUNT_KEY].mean().compute()

    print(f'mean item count: {mean_item_count}')

    # print('item statistics:')
    # print(str(stats))

    # use the same feature encoder for training and regression
    model_seed = random_model_seed()
    print(f'Propensity model seed: {model_seed}')

    features_bag = ddf.map_partitions(encode_partition, meta=(None, 'O')).to_bag()

    del ddf

    features_bag = client.persist(features_bag)

    print('selecting features')
    feature_names = select_features(features_bag)

    print(f'features: {len(feature_names)}, names: {feature_names}')

    # don't use a prior for the propensity string encodings - propensity is memorization, not generalization
    features_bag, string_tables = encode_strings(features_bag, feature_names, model_seed)

    # the propensity is a score between 0.0 and 1.0 so use binary:logistic
    params = {
        'objective': 'binary:logistic',
        'tree_method': 'hist',
        'verbosity': 1
    }

    # allow the features bag to be garbage collected in train_xgb
    wrapped_features_bag = [features_bag]
    del features_bag

    booster = model_utils.train_xgb(client, wrapped_features_bag, feature_names, params=params, num_boost_round=200, early_stopping_rounds=20)
    
    # make sure to use booster.feature_names here since they can be re-ordered by the dataframe during training
    propensity_model = PropensityModel(booster, FeatureEncoder(booster.feature_names, string_tables, model_seed), mean_item_count)
    propensity_model.save(string_tables, model_seed)

    return propensity_model
    
    
def encode_partition(df):

    result = map(encode_for_train, df[DECISION_ID_KEY], df[ITEM_KEY], df[CONTEXT_KEY], df[SAMPLE_KEY], df[COUNT_KEY])
    return pd.Series(itertools.chain.from_iterable(result))  # flatten since each decision row becomes multiple training samples
    

def encode_for_train(decision_id, item, context, sample, count):

    feature_rows = []
    
    context_features = flatten_context(context, into=None)

    unix_timestamp = Ksuid.from_base62(decision_id).datetime.timestamp()

    # add the chosen item with 1.0 weight
    feature_rows.append(encode_item(context_features, item, unix_timestamp))

    # add the sample if any with weight equal to the number of items it was randomly drawn from
    sample_pool_size = count - 1
    if sample_pool_size > 0:
        feature_rows.append(encode_item(context_features, sample, unix_timestamp, chosen=False, weight=sample_pool_size))

    return feature_rows
    
    
def encode_item(context_features, item, unix_timestamp, chosen=True, weight=1.0):
    
    features = flatten_item(item, into=context_features.copy())
    
    assert unix_timestamp >= 0.0 and weight >= 0.0

    features[TIMESTAMP_FEATURE_KEY] = unix_timestamp
    features[TARGET_FEATURE_KEY] = 1.0 if chosen else 0.0
    features[WEIGHT_FEATURE_KEY] = weight

    return features
    

# def get_sample_pool_sizes(df):
#     return map(_get_sample_pool_size, df[COUNT_KEY], df[RUNNERS_UP_KEY])
#
#
# def _get_sample_pool_size(count, runners_up):
#     sample_pool_size = count - 1 - (len(runners_up) if runners_up else 0)  # subtract chosen variant and runners up from count
#     assert sample_pool_size >= 0
#     return sample_pool_size

"""
Design Notes:

2022-11-16: The string target values for givens will all converge to 1/variant_count since the givens are simply replicated for both the chosen and unchosen variants.
This means that the givens string table order will be essentially random and a largely uninformative ordinal encoding. This probably doesn't matter too much
since we mostly care about the overall propensity of the variant, but in some cases it might be nice to have informative givens strings so that propensity can be
accurately matched for things like language or device.

2022-11-17: Propensity models are truly about memorization, not generalization. Overfitting is completely OK because the model is designed to cover the same
records that are being used during decision model training, so inference will essentially be on the training set. Normal concerns about generalization and
performing well against an unseen test set do not apply here. We're not so much doing machine learning as creating a compressed statistical device.
"""