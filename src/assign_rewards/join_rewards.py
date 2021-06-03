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
        #  OLD / ORIG
        # listener_timestamp = listener['timestamp']
        listener_timestamp = listener[constants.TIMESTAMP_KEY]
        if listener_timestamp + window < record_timestamp:
            del listeners[i]
        else:
            listener['reward'] = listener.get('reward',
                                              config.DEFAULT_REWARD_VALUE) + float(
                reward)


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
        #  OLD / ORIG
        # timestamp = x['timestamp']
        # if x['type'] == 'decision':
        #     timestamp -= timedelta(microseconds=1)
        timestamp = x[constants.TIMESTAMP_KEY]
        if x[constants.TYPE_KEY] == constants.DECISION_KEY:
            timestamp -= timedelta(microseconds=1)
        return timestamp

    records.sort(key=sort_key)

    decision_records_by_reward_key = {}
    for record in records:
        # OLD / ORIG
        # if record.get('type') == 'decision':
        if record.get(constants.TYPE_KEY) == constants.DECISION_KEY:
            rewarded_decision = record.copy()

            # OLD / ORIG
            # model = record['model']
            model = record[constants.MODEL_KEY]
            if model not in rewarded_decisions_by_model:
                rewarded_decisions_by_model[model] = []
            rewarded_decisions_by_model[model].append(rewarded_decision)

            reward_key = record.get('reward_key', config.DEFAULT_REWARD_KEY)
            listeners = decision_records_by_reward_key.get(reward_key, [])
            decision_records_by_reward_key[reward_key] = listeners
            listeners.append(rewarded_decision)

        # OLD / ORIG
        # elif record.get('type') == 'rewards':
        elif record.get(constants.TYPE_KEY) == 'rewards':
            for reward_key, reward in record['rewards'].items():
                listeners = decision_records_by_reward_key.get(reward_key, [])
                # OLD / ORIG
                # update_listeners(listeners, record['timestamp'], reward)
                update_listeners(
                    listeners, record[constants.TIMESTAMP_KEY], reward)

        # Event type records get summed to all decisions within the time window regardless of reward_key
        # OLD / ORIG
        # elif record.get('type') == 'event':
        elif record.get(constants.TYPE_KEY) == constants.EVENT_KEY:
            # OLD / ORIG
            # reward = record.get('properties', {}) \
            #                .get('value', config.DEFAULT_EVENT_REWARD_VALUE)
            reward = record.get(constants.PROPERTIES_KEY, {}) \
                .get(constants.VALUE_KEY, config.DEFAULT_EVENT_REWARD_VALUE)
            for reward_key, listeners in decision_records_by_reward_key.items():
                # OLD / ORIG
                # update_listeners(listeners, record['timestamp'], reward)
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


# OLD / ORIG
# def filter_valid_records(hashed_history_id, records):
#     results = []
#
#     history_id = None
#
#     for record in records:
#         try:
#             validate_record(record, history_id, hashed_history_id)
#
#             if not history_id:
#                 # history_id has been validated to hash to the hashed_history_id
#                 history_id = record[constants.HISTORY_ID_KEY]
#
#             results.append(record)
#         except Exception as e:
#             pass
#
#     return results


def validate_record(record, history_id, hashed_history_id):
    # record is a dict (json.loaded)
    if constants.TIMESTAMP_KEY not in record or constants.TYPE_KEY not in record \
            or constants.HISTORY_ID_KEY not in record:
        # raise ValueError('invalid record')
        return False

    # 'decision' type requires a 'model'
    if record[constants.TYPE_KEY] == constants.DECISION_KEY \
            and record.get(constants.MODEL_KEY, None) is None:
        # previously was: \and constants.MODEL_KEY not in record
        # TODO added None validation
        # raise ValueError('invalid record')
        print('type key == `decision` and model name is missing')
        return False

    # validate given type
    givens = record.get(constants.GIVEN_KEY, None)
    if givens is not None and not isinstance(givens, dict):
        # given is not a dict and should be
        print('Givens are not of a dict type')
        return False

    # validate model name
    # ask if model name can be None
    # check valid model name characters
    model_name = record.get(constants.MODEL_KEY, None)

    if model_name is not None:
        # if model name is not None and is not a string that means it is faulty
        if not isinstance(model_name, str):
            print('Model name is not None and not a string')
            return False

        # if model name has special chars like !@#$%^ etc. it is faulty
        if not re.match(constants.MODEL_NAME_REGEXP, model_name):
            print('Model name failed to pass through the regex')
            return False

    if history_id:
        # history id provided, see if they match
        if history_id != record[constants.HISTORY_ID_KEY]:
            print('Provided history_id mismatches the on e in the record')
            return False
            # raise ValueError('history_id hash mismatch')
    elif not utils.hash_history_id(
            record[constants.HISTORY_ID_KEY]) == hashed_history_id:

        print('Record`s history_id mismatches hashed_history_id (?)')
        return False
        # raise ValueError('history_id hash mismatch')
    return True

# OLD / ORIG
# def validate_record(record, history_id, hashed_history_id):
#     if constants.TIMESTAMP_KEY not in record or constants.TYPE_KEY not in record \
#             or constants.HISTORY_ID_KEY not in record:
#         raise ValueError('invalid record')
#
#     # 'decision' type requires a 'model'
#     if record[constants.TYPE_KEY] == constants.DECISION_KEY \
#             and constants.MODEL_KEY not in record:
#         raise ValueError('invalid record')
#
#     # check valid model name characters
#
#     if history_id:
#         # history id provided, see if they match
#         if history_id != record[constants.HISTORY_ID_KEY]:
#             raise ValueError('history_id hash mismatch')
#     elif not utils.hash_history_id(
#             record[constants.HISTORY_ID_KEY]) == hashed_history_id:
#         raise ValueError('history_id hash mismatch')
