import math
from numpy.random import default_rng

from config import EXPLORE

rng = default_rng()

NON_ZERO_POISSON_WEIGHT_PROBABILITY = 1 - 1 / math.e # ~0.63

# if exploring, filter out ~37% of records that would have zero poisson weight the remaining records will be weighted with non-zero poission weights
PARQUET_FILE_SAMPLE = NON_ZERO_POISSON_WEIGHT_PROBABILITY if EXPLORE else 1.0

def exploration_weight():
    return non_zero_poisson_weight() if EXPLORE else 1.0
    

def poisson_weight():
    return rng.poisson(1)


def non_zero_poisson_weight():
    while True:
        weight = poisson_weight()
        if weight > 0:
            return weight