import threading
from datetime import datetime, timedelta
import logging
from numbers import Number
import re
import sys
import signal
import subprocess

import constants
import config
import utils

# Time window to add to a timestamp
window = timedelta(seconds=config.REWARD_WINDOW)


def update_listeners(listeners, record_timestamp, reward):
    if not isinstance(listeners, list):
        raise TypeError("Expecting a list for the 'listeners' arg.")

    if not isinstance(record_timestamp, datetime):
        raise TypeError(
            "Expecting a datetime.datetime timestamp for the 'record_timestamp' arg.")

    if not (isinstance(reward, int) or isinstance(reward, float)):
        raise TypeError("Expecting int, float or bool.")

    # Loop backwards to be able to remove an item in place
    for i in range(len(listeners) - 1, -1, -1):
        listener = listeners[i]
        listener_timestamp = listener[constants.TIMESTAMP_KEY]
        if listener_timestamp + window < record_timestamp:
            del listeners[i]
        else:
            listener['reward'] = listener.get('reward', 0.0) + float(reward)


def assign_rewards_to_decisions(records):
    """
    1) Collect all records of type "decision" in a dictionary.
    2) Assign the rewards of records of type "rewards" to all the "decision"
    records that match two criteria:
      - reward_key
      - a time window
    3) Assign the value of records of type "event" to all "decision" records
    within a time window.

    Args:
        records: a list of records (dicts)

    """
    rewarded_decisions_by_model = {}

    # In the event of a timestamp tie between a decision record and another record, ensure the decision record will be sorted earlier
    # sorting it as if it were 1 microsecond earlier. It is possible that a record 1 microsecond prior to a decision could be sorted after,
    # so double check timestamp ranges for reward assignment
    def sort_key(x):
        timestamp = x[constants.TIMESTAMP_KEY]
        if x[constants.TYPE_KEY] == constants.DECISION_TYPE:
            timestamp -= timedelta(microseconds=1)
        return timestamp
    # sorted(list5, key=lambda vertex: (degree(vertex), vertex))
    records.sort(key=sort_key)

    decision_records_by_reward_key = {}
    for record in records:
        if record.get(constants.TYPE_KEY) == constants.DECISION_TYPE:
            rewarded_decision = record.copy()

            model = record[constants.MODEL_KEY]
            if model not in rewarded_decisions_by_model:
                rewarded_decisions_by_model[model] = []
            rewarded_decisions_by_model[model].append(rewarded_decision)

            reward_key = record.get('reward_key', 'reward')
            listeners = decision_records_by_reward_key.get(reward_key, [])
            decision_records_by_reward_key[reward_key] = listeners
            listeners.append(rewarded_decision)

        elif record.get(constants.TYPE_KEY) == 'rewards':
            for reward_key, reward in record['rewards'].items():
                listeners = decision_records_by_reward_key.get(reward_key, [])
                update_listeners(
                    listeners, record[constants.TIMESTAMP_KEY], reward)

        # Event type records get summed to all decisions within the time window regardless of reward_key
        elif record.get(constants.TYPE_KEY) == constants.EVENT_TYPE:
            reward = record.get(constants.PROPERTIES_KEY, {}) \
                .get(constants.VALUE_KEY, config.DEFAULT_EVENT_REWARD_VALUE)
            for reward_key, listeners in decision_records_by_reward_key.items():
                update_listeners(
                    listeners, record[constants.TIMESTAMP_KEY], reward)

    return rewarded_decisions_by_model


def filter_valid_records(hashed_history_id, records):
    results = []

    history_id = None

    for record in records:
        is_record_valid = \
            validate_record(record, history_id, hashed_history_id)

        if not history_id:
            # history_id has been validated to hash to the hashed_history_id
            history_id = record[constants.HISTORY_ID_KEY]

        if is_record_valid:
            results.append(record)

    return results


def validate_record(record, history_id, hashed_history_id):
    # record is a dict (json.loaded)
    if constants.TIMESTAMP_KEY not in record or constants.TYPE_KEY not in record \
            or constants.HISTORY_ID_KEY not in record:
        return False

    # 'decision' type requires a 'model'
    if record[constants.TYPE_KEY] == constants.DECISION_TYPE \
            and record.get(constants.MODEL_KEY, None) is None:
        return False

    # validate given type
    givens = record.get(constants.GIVEN_KEY, None)
    if givens is not None and not isinstance(givens, dict):
        # given is not a dict and should be
        return False

    # validate model name
    # ask if model name can be None
    # check valid model name characters
    model_name = record.get(constants.MODEL_KEY, None)

    if model_name is not None:
        # if model name is not None and is not a string that means it is faulty
        if not isinstance(model_name, str):
            # model name is not an instance of a string
            return False

        # if model name has special chars like !@#$%^ etc. it is faulty
        if not re.match(constants.MODEL_NAME_REGEXP, model_name):
            # model name fails to pass regexp test
            return False

    if history_id:
        # history id provided, see if they match
        if history_id != record[constants.HISTORY_ID_KEY]:
            # input history id mismatches the one of the record
            return False
    elif not utils.hash_history_id(
            record[constants.HISTORY_ID_KEY]) == hashed_history_id:
        # Record`s history_id mismatches hashed_history_id (?)
        return False
    return True
