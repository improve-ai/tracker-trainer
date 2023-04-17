import pandas as pd
import random

from config import should_sample_context, MAX_TREES, TREE_DEPTH, MODEL_PATH, REWARDED_DECISIONS_PATH, MAX_DECISION_RECORDS, REWARD_PRIOR_COUNT, NORMALIZE_REWARDS, BINARY_REWARDS
from constants import DECISION_ID_KEY, ITEM_KEY, CONTEXT_KEY, REWARD_KEY, TARGET_FEATURE_KEY, WEIGHT_FEATURE_KEY
from model_utils import transform_model, train_xgb
from parquet_io import DataFrameLoader
from exploration import PARQUET_FILE_SAMPLE, exploration_weight
from propensities import PropensityModel
from utils import random_model_seed
from feature_selection import select_features
from feature_flattener import flatten_item, flatten_context
from string_encoder import encode_strings
from feature_encoder import get_noise_shift_scale, sprinkle

# differentiate the name from the core DecisionModel class that we use for making decisions
class TrainedDecisionModel:
    
    def __init__(self, booster, string_tables: dict, model_seed: int):
        
        self.booster = booster
        self.string_tables = string_tables
        self.model_seed = model_seed
        # this data loader will also be used during model evaluation ->
        # files used to train DecisionModel should be skipped


    def save(self):

        booster, mlmodel = transform_model(booster=self.booster, string_tables=self.string_tables, model_seed=self.model_seed)

        mlmodel_path = MODEL_PATH / 'model.mlmodel'
        mlmodel.save(mlmodel_path)

        booster_path = MODEL_PATH / 'model.xgb'
        booster.save_model(booster_path)


def train(client, propensity_model: PropensityModel):
    
    print('starting final training')
    
    # on load, if exploring, filter out ~37% of records that would have zero poisson weight the remaining records will be weighted with non-zero poission weights in encode_features
    loader = DataFrameLoader(client, parquet_path=REWARDED_DECISIONS_PATH, max_rows=MAX_DECISION_RECORDS, sample=PARQUET_FILE_SAMPLE)
    # it is important that if exploring ~37% of records are filtered out, regardless of the number of rows
    assert loader.min_rows == 0

    ddf = loader.load(columns=[DECISION_ID_KEY, ITEM_KEY, CONTEXT_KEY, REWARD_KEY])

    if BINARY_REWARDS:
        ddf[REWARD_KEY] = (ddf[REWARD_KEY] > 0.0).astype(float)

    # ddf.describe() is probably the most efficient way to compute multiple statistics at once
    stats = ddf[REWARD_KEY].describe().compute()

    mean = stats.loc['mean']
    std = stats.loc['std']

    print('reward statistics:')
    print(str(stats))

    feature_names = propensity_model.selected_features()

    print(f'features count: {len(feature_names)}, names: {feature_names}')

    # flatten features, compute bootstrap and propensity weights, normalize rewards
    features_bag = ddf.map_partitions(encode_partition, propensity_model, mean, std, meta=(None, 'O')).to_bag()

    del ddf

    features_bag = client.persist(features_bag)

    model_seed = random_model_seed()

    # by definition, normalization shifts the mean to 0.0
    normalized_mean = 0.0
    if not NORMALIZE_REWARDS:
        normalized_mean = mean

    features_bag, string_tables = encode_strings(features_bag, feature_names, model_seed, prior_mean=normalized_mean, prior_count=REWARD_PRIOR_COUNT)

    # by encoding a random population id (noise) it is possible to still get some sub-population splits even within this single bootstrap replica
    features_bag = features_bag.map(encode_random_population_id)

    params = { 'objective': 'reg:squarederror',
               'tree_method': 'hist',
               'max_depth': TREE_DEPTH,
               'verbosity': 1 }
    
    # allow the features bag to be garbage collected in train_xgb
    wrapped_features_bag = [features_bag]
    del features_bag

    booster = train_xgb(client, wrapped_features_bag, feature_names, params=params, num_boost_round=MAX_TREES, early_stopping_rounds=None)

    return TrainedDecisionModel(booster, string_tables, model_seed)


def encode_partition(df, propensity_model: PropensityModel, reward_mean: float, reward_std: float):

    # avoid divide by zero when all rewards are identical    
    if reward_std == 0:
        reward_std = 1

    results = []
    
    normalized_inverse_propensity_weights = propensity_model.normalized_inverse_propensity_weights(df)

    for item, context, reward, normalized_inverse_propensity_weight in zip(df[ITEM_KEY], df[CONTEXT_KEY], df[REWARD_KEY], normalized_inverse_propensity_weights):
        # the inverse propensity weight balances the weights of the items so that the model doesn't overfit to the popular items
        # the poisson weight turns this into a single bootstrapped model out of a virtual ensemble of recently trained models
        # if exploring, zero poisson weight records were, effectively, already removed by filtering out ~37% of the records
        # ~37% of the poisson weights will be 1, the remaining ~26% will be > 1
        weight = normalized_inverse_propensity_weight * exploration_weight()

        # normalize the reward
        normalized_reward = ((reward - reward_mean) / reward_std)

        context = context if should_sample_context() else None
    
        flat_features = {}

        # TODO check out pd.normalize_json()
        flatten_context(context, into=flat_features)
        flatten_item(item, into=flat_features)

        flat_features[WEIGHT_FEATURE_KEY] = weight
        
        flat_features[TARGET_FEATURE_KEY] = normalized_reward if NORMALIZE_REWARDS else reward

        # print(f'reward: {reward} normalized: {normalized_reward} weight: {weight} item: {item} context: {context}')
        
        results.append(flat_features)

    return pd.Series(results)

 
def encode_random_population_id(features: dict):
    # use random value in [0.0, 1.0), which we can think of as a random 6-7 bit population id
    noise_shift, noise_scale = get_noise_shift_scale(random.random())
    result = {}

    excluded_keys = set([TARGET_FEATURE_KEY, WEIGHT_FEATURE_KEY])

    # every feature for this decision is encoded with the same id
    for feature_name, value in features.items():
        if feature_name not in excluded_keys:
            value = sprinkle(value, noise_shift=noise_shift, noise_scale=noise_scale)
        result[feature_name] = value

    return result

'''
Notes:

2022-10-31: We had to disable 'colsample_bytree': 0.8 because this caused simple cases with small numbers of items and context to not be able to do a item and the context in the same tree


'''