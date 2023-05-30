import math
from collections import namedtuple

import xxhash

from constants import WEIGHT_FEATURE_KEY, TARGET_FEATURE_KEY
from config import MAX_STRINGS_PER_FEATURE, PRUNE_MIN_STRING_COUNT
from feature_encoder import StringTable

xxh3 = xxhash.xxh3_64_intdigest

StringStats = namedtuple('StringStats', ['weight', 'target', 'count'])
empty_stats = StringStats(weight=0, target=0, count=0)

class StringEncoder:
    """
    Encodes mixed flattened features in to float features
    """
    def __init__(self, string_tables: dict, model_seed: int):
        """
        Initialize the feature encoder

        Parameters
        ----------
        string_tables: dict
            a mapping from feature names to string hash tables
        model_seed: int
            model seed to be used during string encoding

        Raise
        ----------
        ValueError if feature names or tables are corrupt
        """

        # initialize string encoding tables
        self.string_tables = {}
        self.empty_table = StringTable([], model_seed)

        for feature_name, table in string_tables.items():
            self.string_tables[feature_name] = StringTable(table, model_seed)


    def encode_strings(self, flattened_features):
        result = {}

        # TODO, maybe returning tuples instead of dicts would be more performant for further converting to dataframe
        for feature_name, value in flattened_features.items():
            if isinstance(value, str):
                string_table = self.string_tables.get(feature_name)
                if string_table is None:
                    string_table = self.empty_table

                value = string_table.encode(value)

            result[feature_name] = value

        return result


def encode_strings(flat_features_bag, feature_names: list, model_seed: int, prior_mean=0.0, prior_count=0):
    string_tables = construct_string_tables(flat_features_bag, feature_names, model_seed, prior_mean, prior_count)
    string_encoder = StringEncoder(string_tables, model_seed)
    return (flat_features_bag.map(string_encoder.encode_strings), string_tables)


def construct_string_tables(flat_features_bag, allowed_feature_names: list, model_seed: int, prior_mean: float, prior_count: int):

    sorted_strings_by_feature_name = get_sorted_strings_by_feature_name(flat_features_bag, prior_mean=prior_mean, prior_count=prior_count)

    allowed_feature_names_set = set(allowed_feature_names)

    string_tables = {}
    for feature_name, sorted_strings in sorted_strings_by_feature_name.items():
        # ensure the feature name is in the set of allowed feature names
        if feature_name not in allowed_feature_names_set:
            continue

        if len(sorted_strings) == 0:
            continue

        hashes = None
        # there needs to be at least log2(N) bits to uniquely store N entries
        for n_bits in range(max(int(math.log2(len(sorted_strings))),1), 64):
            hashes = list(map(lambda x: hash(x, n_bits=n_bits, seed=model_seed), sorted_strings))
            if len(sorted_strings) == len(set(hashes)):
                # if the lengths are different there was a collision, add a bit
                break

        string_tables[feature_name] = hashes

    print(f'string tables {string_tables}')
    print(f'string counts: {({k: len(v) for k, v in string_tables.items()})}')
    return string_tables


def hash(string: str, n_bits: int, seed: int):
    # n_bits in current code will never exceed 63
    # assert that n_bits > 0 and n_bits < 64
    assert 0 < n_bits < 64
    mask = (1 << (n_bits + 1)) - 1
    return xxh3(string, seed) & mask


def get_sorted_strings_by_feature_name(flat_features_bag, prior_mean: float, prior_count: int):
    stats_by_string_by_feature_name = flat_features_bag.reduction(string_stats_chunk, string_stats_aggregate).compute()

    result = {}
    for feature_name, stats_by_string in stats_by_string_by_feature_name.items():

        stats_by_string = maybe_prune(feature_name, stats_by_string)

        # TODO maybe add small tiebreaker
        sorted_items = sorted(stats_by_string.items(), key=lambda x: mean_target_with_prior(x[1], prior_mean=prior_mean, prior_count=prior_count), reverse=True)
        for item in sorted_items:
            string, stats = item
            print(f'{feature_name}:{string} mean {mean_target_with_prior(stats, 0, 0)} mean w/prior {mean_target_with_prior(stats, prior_mean=prior_mean, prior_count=prior_count)} prior_mean {prior_mean} prior_count {prior_count} target {stats.target} weight {stats.weight} count {stats.count}')

        result[feature_name] = list(dict(sorted_items).keys())

    print(f'string_tables {result}')
    return result


def mean_target_with_prior(stats: StringStats, prior_mean: float, prior_count: int):
    prior_weight = prior_count * stats.weight / stats.count
    updated_weight = stats.weight + prior_weight
    updated_target = stats.target + (prior_mean * prior_weight)
    return updated_target / updated_weight


def maybe_prune(feature_name, stats_by_string):
    sorted_items = sorted(stats_by_string.items(), key=lambda x: x[1].weight, reverse=True)

    # if count is too small, prune it
    sorted_items = list(filter(lambda x: x[1].count > PRUNE_MIN_STRING_COUNT, sorted_items))

    # limit to MAX_STRINGS_PER_FEATURE regardless of pruning
    sorted_items = sorted_items[:MAX_STRINGS_PER_FEATURE]

    if len(sorted_items) < len(stats_by_string): 
        print(f'pruned {len(stats_by_string) - len(sorted_items)} strings from {feature_name}')

    return dict(sorted_items)


def string_stats_chunk(seq):
    stats_by_string_by_feature_name = {}
    for x in seq:
        weight = x.get(WEIGHT_FEATURE_KEY, 1.0)
        target = x.get(TARGET_FEATURE_KEY, 0.0)

        for feature_name, feature_value in x.items():
            if not isinstance(feature_value, str):
                continue

            stats_by_string = stats_by_string_by_feature_name.get(feature_name, None)
            if stats_by_string is None:
                stats_by_string = {}
                stats_by_string_by_feature_name[feature_name] = stats_by_string

            totals = stats_by_string.get(feature_value, empty_stats)
            # target is multiplied by weight, count is 1
            stats_by_string[feature_value] = StringStats(weight=totals.weight+weight, target=totals.target+(target * weight), count=totals.count+1)

    return stats_by_string_by_feature_name


def string_stats_aggregate(dicts):
    stats_by_string_by_feature_name = {}
    for d in dicts:
        for feature_name, merge_stats_by_string in d.items():
            stats_by_string = stats_by_string_by_feature_name.get(feature_name, None)
            if stats_by_string is None:
                stats_by_string = {}
                stats_by_string_by_feature_name[feature_name] = stats_by_string

            for string, stats in merge_stats_by_string.items():
                totals = stats_by_string.get(string, empty_stats)
                stats_by_string[string] = StringStats(weight=totals.weight+stats.weight, target=totals.target+stats.target, count=totals.count+stats.count)

    return stats_by_string_by_feature_name

"""
Design Notes:
2022-11-16: Initially I'm not doing any prior since it will add complexity and is somewhat different than thompson sampling since it will pull
small weight values toward the mean. This is probably the right thing to do in the long run, but its a little complicated - for the 
propensity case the weights have a mean of variant_count rather than 1.

2022-11-17: An argument for not using priors for the propensity model:
Consider a string variant that is only used once and is chosen that time. Its propensity target will be 1.0. The only purpose of its propensity
score is to match that same variant for determining it's weight as a decision record. It was only used once, so the fact that its
propensity will be high and thus it's weight small is totally fine because it should contribute nothing to the model anyway, so this
is a highly accurate target value and thus will help the propensity model fit well.

2022-11-28: Pruning small count strings seems to be the simplest, easiest to understand pruning mechanism. Small count strings will tend to be close to the prior,
so this effectively prunes values near the prior without trying to figure out some magic prior standard deviation cutoff value. For the propensity model,
which doesn't use a prior, we could be pruning values that aren't sampled often, this might not be totally ideal since low propensity should be toward
the bottom of the list rather than near median.
# TODO, another issue here is that since the mean is not weighted, the median value of the string table will actually not be at 0.0, so everything will
be shifted a bit for the missing items. This probably should be fixed.
"""