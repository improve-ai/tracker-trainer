from constants import WEIGHT_FEATURE_KEY, TARGET_FEATURE_KEY
from config import MAX_FEATURES


def select_features(flattened_features_bag):
    feature_weights = flattened_features_bag.reduction(feature_weights_chunk, feature_weights_aggregate).compute()

    sorted_items = sorted(feature_weights.items(), key=lambda x: x[1], reverse=True)

    # prune to MAX_FEATURES
    sorted_items = sorted_items[:MAX_FEATURES]

    if len(sorted_items) < len(feature_weights): 
        print(f'pruned {len(feature_weights) - len(sorted_items)} features')

    return list(dict(sorted_items).keys())
    

def feature_weights_chunk(seq):
    weights = {}
    for x in seq:
        weight = x.get(WEIGHT_FEATURE_KEY, 1.0)

        for feature_name in x:
            weights[feature_name] = weights.get(feature_name, 0) + weight

    return weights


def feature_weights_aggregate(dicts):
    exclude_feature_names = set([WEIGHT_FEATURE_KEY, TARGET_FEATURE_KEY])

    weights = {}
    for d in dicts:
        for feature_name, weight in d.items():
            if feature_name in exclude_feature_names:
                continue

            weights[feature_name] = weights.get(feature_name, 0) + weight

    return weights