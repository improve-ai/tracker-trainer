import math
import time

from collections import defaultdict
from typing import Any, Dict, Sequence  # , Optional, cast, Hashable

import numpy as np
from coba.primitives.semantic import Context, Action
from coba.learners.primitives import Learner

import requests_mock

from improveai import RewardTracker


from firehose_record import FirehoseRecord

from train import train_model

TRACK_URL = 'http://dummy.url.com'


class ImproveAILearner(Learner):

    def __init__(self, hyperparameters={}, decisions_per_epoch=16, epoch_scale=2.0, rng=None, np_rng=None) -> None:

        self._hyperparameters = hyperparameters
        self._decisions_per_epoch = decisions_per_epoch
        self._epoch_scale = epoch_scale
        self._epoch_decisions = 0
        self.rng = rng
        self.np_rng = np_rng
        
        # Valid ImproveAI model will be available after first iteration
        # Can't instantiate a scorer without a model URL so the _scorer is initially set to None
        # When predicting check if scorer is not None -> if it is None choose a random item
        self._scorer = None
        self._tracker = RewardTracker(model_name='benchmark', track_url=TRACK_URL, _threaded_requests=False)
        self._firehose_records = []


    @property
    def params(self) -> Dict[str, Any]:
        result = {"family": "ImproveAI"}
        result.update(self._hyperparameters)
        return result


    def predict(self, context: Context, actions: Sequence[Action]):
        with requests_mock.Mocker() as m:
            m.post(TRACK_URL, text='success', additional_matcher=self.log_track)
            if self._scorer is not None:
                # choose best action according to scorer and track it
                scores = self._scorer.score(items=actions, context=context)
                # get best score
                best_action_index = np.argmax(scores)
                action = actions[best_action_index]
            else:
                # choose random action and track it
                # Use numpy random generator to achieve full reproducibility
                best_action_index = self.np_rng.integers(0, len(actions))
                action = actions[best_action_index]

            # track with reward tracker
            decision_id = self._tracker.track(item=action, candidates=actions, context=context)

        probs = [0.0] * len(actions)
        probs[best_action_index] = 1.0
        assert decision_id is not None

        return (probs, decision_id)

    def learn(self, context: Context, action: Action, reward: float, probability: float, decision_id: str, **kwargs) -> None:

        # TODO check that action matches the last action. The meta-learner will sometimes train on
        #  non-chosen actions.

        if decision_id:
            with requests_mock.Mocker() as m:
                m.post(TRACK_URL, text='success', additional_matcher=self.log_track)
                self._tracker.add_reward(reward=reward, reward_id=decision_id)

        self._epoch_decisions += 1
        
        if self._epoch_decisions >= self._decisions_per_epoch:
            self._epoch_decisions = 0
            self._decisions_per_epoch *= self._epoch_scale
            self._scorer = train_model(self._firehose_records, extra_hyperparameters=self._hyperparameters)
            self._tracker.track_url = TRACK_URL

    def log_track(self, request):
        # list append is thread safe
        self._firehose_records.append(FirehoseRecord(request.json()))
        #print(request.json())
        return True

