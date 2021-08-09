from datetime import timedelta
import re

import constants
import config
import utils
import customize

# Time window to add to a timestamp
window = timedelta(seconds=config.REWARD_WINDOW)

def assign_rewards(history: list):
    """
    Args:
        history: a list of records (dicts) (in/out)
    """
    rewarded_decisions_by_model = {}
    for record in history:
        if record.get(constants.TYPE_KEY) == constants.DECISION_TYPE:
            rewarded_decision = record.copy()

            model = record[constants.MODEL_KEY]
            if model not in rewarded_decisions_by_model:
                rewarded_decisions_by_model[model] = []

            rewarded_decisions_by_model[model].append(rewarded_decision)

    rewardable_decisions = []
    
    for record in history:
        if record.get(constants.TYPE_KEY) == constants.DECISION_TYPE:
            rewardable_decisions.append(record)
        elif record.get(constants.TYPE_KEY) == constants.EVENT_TYPE:
            reward = record.get(constants.PROPERTIES_KEY, {}).get(constants.VALUE_KEY, config.DEFAULT_EVENT_REWARD_VALUE)
            # Loop backwards to be able to remove an item in place
            for i in range(len(rewardable_decisions) - 1, -1, -1):
                listener = rewardable_decisions[i]
                listener_timestamp = listener[constants.TIMESTAMP_KEY]
                if listener_timestamp + window < record_timestamp:
                    del rewardable_decisions[i]
                else:
                    listener['reward'] = listener.get('reward', 0.0) + float(reward)


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
    
    # TODO validate 'reward', 'properties.value'
