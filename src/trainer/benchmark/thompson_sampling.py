import random
import numpy as np

from typing import Any, Dict, Sequence, Optional, cast, Hashable

from coba.environments import Context, Action
from coba.statistics import OnlineVariance
from coba.learners.primitives import Learner

# TODO, I've seen this get stuck performing very badly on HappySundayStrings a few times, probably some problem with the prior
class ThompsonSamplingLearner(Learner):

    def __init__(self, decisions_per_epoch = 64, n_bootstraps = 128) -> None:

        self._decisions_per_epoch = decisions_per_epoch
        self._n_bootstraps = n_bootstraps
        self._bootstraps = {}
        self._total_reward = 0.0
        self._n_decisions = 0.0
        self._new_decisions = []


    def predict(self, context: Context, actions: Sequence[Action]):

        scores = []

        for action in actions:
            score = None

            bootstraps = self._bootstraps.get(action)
            if bootstraps is not None:
                reward, pulls = random.choice(bootstraps)
                if pulls > 0:
                    score = reward / pulls

            if score is None:
                if self._n_decisions > 0:
                    score = self._total_reward / self._n_decisions
                else:
                    score = 1.0    

            score += random.random() * 2**-22
            scores.append(score)

        probs = [0.0] * len(actions)
        probs[scores.index(max(scores))] = 1.0

        return probs

    def learn(self, context: Context, actions: Sequence[Action], action: Action, reward: float, probability: float, **kwargs) -> None:

        self._new_decisions.append((action, reward))

        if len(self._new_decisions) == self._decisions_per_epoch:
            for (action, reward) in self._new_decisions:

                bootstraps = self._bootstraps.get(action)

                if bootstraps is None:
                    bootstraps = [(0.0, 0)] * self._n_bootstraps
                    self._bootstraps[action] = bootstraps

                for i in range(self._n_bootstraps):
                    weight = np.random.poisson(1.0)
                    if weight == 0.0:
                        continue
                    oldreward, pulls = bootstraps[i]
                    bootstraps[i] = (oldreward + (weight * reward), pulls + weight)

                self._total_reward += reward
                self._n_decisions += 1

            self._new_decisions = []