"""
NAME
Script that ...
-------------------------------------------------------------------------------
Usage:
    ...
"""

# Built-in imports
from datetime import datetime
from datetime import timedelta
import logging
import json
import gzip

# External imports
# None so far

# Local imports
from utils import load_sorted_records
from config import INPUT_FILENAME
from config import OUTPUT_FILENAME
from config import REWARD_WINDOW_SECONDS
from config import DEFAULT_REWARD_KEY
from config import DATETIME_FORMAT
from config import DEFAULT_EVENTS_REWARD_VALUE
from config import DEFAULT_REWARD_VALUE
from exceptions import InvalidType


# Setup logging
logging.basicConfig(level=logging.DEBUG)

# Time window to add to a timestamp
window = timedelta(seconds=REWARD_WINDOW_SECONDS) 


def update_listeners(listeners, record, reward):
    """
    Update the reward property value of each of the given list of records, 
    in place.
    
    Args:
        listeners: list
        record   : dict
        reward   : number
    
    Returns:
        None
    """

    # Loop backwards to be able to remove an item in place
    for i in range(len(listeners)-1, -1, -1):
        listener = listeners[i]
        listener_timestamp = datetime.strptime(listener['timestamp'], DATETIME_FORMAT)
        record_timestamp = datetime.strptime(record['timestamp'], DATETIME_FORMAT)
        if listener_timestamp + window < record_timestamp:
            logging.debug(f'Deleting listener...')
            del listeners[i]
        else:
            logging.debug(f'Adding reward of {float(reward)} to decision')
            listener['reward'] = (listener.get('reward', DEFAULT_REWARD_VALUE)) + float(reward)


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
        records: a list of records (dicts) sorted by their "timestamp" property.

    Returns:
        dict whose keys are 'reward_key's and its values are lists of records.
    
    Raises:
        InvalidType: If a record has an invalid type attribute.
    """

    decision_records_by_reward_key = {}
    for record in records:
        if record.get('type') == 'decision':
            reward_key = record.get('reward_key', DEFAULT_REWARD_KEY)
            listeners = decision_records_by_reward_key.get(reward_key, [])
            decision_records_by_reward_key[reward_key] = listeners
            listeners.append(record)
        
        elif record.get('type') == 'rewards':
            for reward_key, reward in record['rewards'].items():
                listeners = decision_records_by_reward_key.get(reward_key, [])
                logging.debug(f"Working on reward_key: '{reward_key}'")
                update_listeners(listeners, record, reward)
        
        elif record.get('type') == 'event':
            reward = record.get('properties', { 'properties': {} }) \
                           .get('value', DEFAULT_EVENTS_REWARD_VALUE)
            # Update all stored records
            for reward_key, listeners in decision_records_by_reward_key.items():
                update_listeners(listeners, record, reward)
            
        else:
            raise InvalidType
    
    return decision_records_by_reward_key


def gzip_records(filename, rewarded_records):
    """
    Create a gzipped jsonlines file with the rewarded-decisions records.
    
    Args:
        filename        : name of the output file without the ".jsonl.gz" part
        rewarded_records: a dict as returned by assign_rewards_to_decisions
    """

    output_filename = OUTPUT_FILENAME.format(filename)
    try:
        with gzip.open(output_filename, mode='wt') as gzf:
            for reward_key, records in rewarded_records.items():
                for record in records:
                    gzf.write((json.dumps(record) + "\n"))
    except Exception as e:
        logging.error(
            f'An error occurred while trying to write the gzipped file:'
            f'{output_filename}'
            f'{e}')
        raise e


def worker():
    """
    
    """
    
    rewarded_records = load_sorted_records(INPUT_FILENAME)

    rewarded_records = assign_rewards_to_decisions(rewarded_records)

    gzip_records('test', rewarded_records)

    # overwrite output file
    return rewarded_records

if __name__ == "__main__":
    logging.info("Starting a new run...")
    records = worker()