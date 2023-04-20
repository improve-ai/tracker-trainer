import os
import sys

from benchmark_config import get_ready_for_benchmark_run

get_ready_for_benchmark_run()

import numpy as np
from tkinter import E
from coba.environments import Environments
from coba.learners     import RandomLearner, EpsilonBanditLearner, LinUCBLearner, UcbBanditLearner
from coba.experiments  import Experiment
from coba.contexts import CobaContext
from coba.random import CobaRandom

from learner import ImproveAILearner
from thompson_sampling import ThompsonSamplingLearner
from simulations import HappySunday, HappySundayStrings, LinearValueMatcher, NonLinearValueMatcher, FeatureMatcher, CompareValuePairs, CompareFeaturePairs




if __name__ == '__main__':

    CobaContext.cacher.cache_directory = './.coba_cache'

    #https://www.openml.org/search?type=data&sort=runs&id=3&status=active
    king_rook_vs_king_pawn = 3
    spambase = 44
    gene_splice = 46
    electricity = 151
    forest_cover_type = 293 # 180, 1596
    pokerhand = 155 # 354 also
    drama = 273
    mnist = 554
    kddcup = 1110 # ~5M instances
    musk = 1116

    medium = [6, 32, 151, 180, 273, 279, 727, 821, 822, 823, 846, 881, 901, 977, 1019, 1040, 1044, 1046, 1056, 1120, 1242, 1471, 1486, 1596]
    bigger = [150, 153, 154, 155, 156, 161, 162, 293, 351, 357, 1110, 1113, 1169, 1216, 1217, 1218, 1241, 1483]
    many_features = [mnist, 1077, 1078, 1079, 1080, 1081, 1082, 1083, 1084, 1085, 1086, 1087, 1088, 1104] # LinUCB is too slow on these

    rng = CobaRandom(seed=0)

    #env = Environments.from_linear_synthetic(n_interactions=10000,n_actions=10, seed=5)
    #env = Environments.from_neighbors_synthetic(n_interactions=3000)
    #env = Environments.from_kernel_synthetic(n_interactions=30000)
    #env = Environments.from_mlp_synthetic(n_interactions=3000)
    #env = Environments.from_openml([king_rook_vs_king_pawn, spambase, gene_splice, electricity, forest_cover_type], take=3000 )
    #env = Environments.from_openml(medium, take=10000)
    #env = Environments.from_openml(bigger, take=100000)
    #env = Environments.from_openml(huge_features, take=3000)
    env = [
        HappySundayStrings(n_interactions=10000, rng=rng),
        HappySunday(n_interactions=10000, rng=rng),
        LinearValueMatcher(n_interactions=10000),
        NonLinearValueMatcher(n_interactions=10000, rng=rng),
        FeatureMatcher(n_interactions=10000, rng=rng),
        CompareValuePairs(n_interactions=10000, rng=rng),
        CompareFeaturePairs(n_interactions=10000, rng=rng) ]

    env = [HappySundayStrings(n_interactions=5000, rng=rng)]
    # lrn = [ ImproveAILearner(), ThompsonSamplingLearner(), RandomLearner(), EpsilonBanditLearner(), UcbBanditLearner(), LinUCBLearner()]

    lrn = [
        ImproveAILearner(rng=rng, np_rng=np.random.default_rng(seed=0)),
        ThompsonSamplingLearner(),
        EpsilonBanditLearner(),
        UcbBanditLearner(), RandomLearner()]

    result = Experiment(env, lrn).evaluate()
    # result.plot_learners()
    result.plot_learners(ylim=(0, 0.2))