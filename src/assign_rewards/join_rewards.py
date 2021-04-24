"""
AWS Batch worker
This script is intended to be used from inside a Docker container to process
jsonl files.
-------------------------------------------------------------------------------
Usage:
    python join_rewards.py
"""

# Built-in imports
from datetime import timedelta
from datetime import datetime
import logging
import sys
import signal
import subprocess


# stats constants
TOTAL_INCOMING_FILE_COUNT = "Incoming Files (Total)"
PROCESSED_INCOMING_FILE_COUNT = "Incoming Files Processed"
PROCESSED_HISTORY_FILE_COUNT = "History Files Processed"

# Time window to add to a timestamp
window = timedelta(seconds=REWARD_WINDOW)


def update_listeners(listeners, record_timestamp, reward):
    """
    Update the reward property value of each of the given list of records, 
    in place.
    
    Args:
        listeners        : list of dicts
        record_timestamp : A datetime.datetime object
        reward           : int or float
    
    Returns:
        None
    
    Raises:
        TypeError if an unexpected type is received
    """

    if not isinstance(listeners, list):
        raise TypeError("Expecting a list for the 'listeners' arg.")

    if not isinstance(record_timestamp, datetime):
        raise TypeError("Expecting a datetime.datetime timestamp for the 'record_timestamp' arg.")

    if not (isinstance(reward, int) or isinstance(reward, float)):
        raise TypeError("Expecting int, float or bool.")


    # Loop backwards to be able to remove an item in place
    for i in range(len(listeners)-1, -1, -1):
        listener = listeners[i]
        listener_timestamp = listener['timestamp']
        if listener_timestamp + window < record_timestamp:
            del listeners[i]
        else:
            listener['reward'] = listener.get('reward', DEFAULT_REWARD_VALUE) + float(reward)


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
        timestamp = x['timestamp']     
        if x['type'] == 'decision':
            timestamp -= timedelta(microseconds=1)
        return timestamp

    records.sort(key=sort_key)

    decision_records_by_reward_key = {}
    for record in records:
        if record.get('type') == 'decision':
            rewarded_decision = record.copy()
            
            model = record['model']
            if not model in rewarded_decisions_by_model:
                rewarded_decisions_by_model[model] = []
            rewarded_decisions_by_model[model].append(rewarded_decision)

            reward_key = record.get('reward_key', DEFAULT_REWARD_KEY)
            listeners = decision_records_by_reward_key.get(reward_key, [])
            decision_records_by_reward_key[reward_key] = listeners
            listeners.append(rewarded_decision)
        
        elif record.get('type') == 'rewards':
            for reward_key, reward in record['rewards'].items():
                listeners = decision_records_by_reward_key.get(reward_key, [])
                update_listeners(listeners, record['timestamp'], reward)
        
        # Event type records get summed to all decisions within the time window regardless of reward_key
        elif record.get('type') == 'event':
            reward = record.get('properties', {}) \
                           .get('value', DEFAULT_EVENTS_REWARD_VALUE)
            for reward_key, listeners in decision_records_by_reward_key.items():
                update_listeners(listeners, record['timestamp'], reward)
            

    return rewarded_decisions_by_model


def filter_valid_records(hashed_history_id, records):

    results = []
    
    history_id = None
    skipped_record_count = 0

    for record in records:
        try:
            validate_record(record, history_id, hashed_history_id)

            if not history_id:
                # history_id has been validated to hash to the hashed_history_id
                history_id = record[HISTORY_ID_KEY]

            results.append(record)
        except Exception as e:
            skipped_record_count += 1
        
    if skipped_record_count:
        print(f'skipped {skipped_record_count} invalid records for hashed history_id {hashed_history_id}')
        
    return results 

def validate_record(record, history_id, hashed_history_id):
    if not TIMESTAMP_KEY in record or not TYPE_KEY in record or not HISTORY_ID_KEY in record:
        raise ValueError('invalid record')

    # 'decision' type requires a 'model'
    if record[TYPE_KEY] == DECISION_KEY and not MODEL_KEY in record:
        raise ValueError('invalid record')
        
    # TODO check valid model name characters
    
    if history_id:
        # history id provided, see if they match
        if history_id != record[HISTORY_ID_KEY]:
            raise ValueError('history_id hash mismatch')
    elif not hash_history_id(record[HISTORY_ID_KEY]) == hashed_history_id:
        raise ValueError('history_id hash mismatch')
