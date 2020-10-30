"""
NAME
Script that ...
-------------------------------------------------------------------------------
Usage:
    python worker.py
"""

# Built-in imports
from datetime import timedelta
from datetime import datetime
from base64 import b64decode
import logging
import json
import gzip
import os

# External imports
# None so far

# Local imports
from utils import load_sorted_records
from config import OUTPUT_FILENAME
from config import DEFAULT_REWARD_KEY
from config import DATETIME_FORMAT
from config import DEFAULT_EVENTS_REWARD_VALUE
from config import DEFAULT_REWARD_VALUE
from config import PATH_INPUT_DIR
from config import PATH_OUTPUT_DIR
from config import LOGGING_LEVEL
from config import LOGGING_FORMAT
from exceptions import InvalidType


# Setup logging
logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)

# Time window to add to a timestamp
window = timedelta(seconds=int(os.environ['DEFAULT_REWARD_WINDOW_IN_SECONDS']))


def update_listeners(listeners, record, reward):
    """
    Update the reward property value of each of the given list of records, 
    in place.
    
    Args:
        listeners: list
        record   : dict, either with a property 'type'='reward' or 'type'='event'
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
            logging.debug(f'Deleting listener: {listener_timestamp}, Reward/event: {record_timestamp}')
            del listeners[i]
        else:
            # logging.debug(f'Adding reward of {float(reward)} to decision.')
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


def gzip_records(input_file, rewarded_records):
    """
    Create a gzipped jsonlines file with the rewarded-decisions records.
    
    Args:
        input_file      : Path object towards the input jsonl file
        rewarded_records: a dict as returned by assign_rewards_to_decisions
    """
    output_file = PATH_OUTPUT_DIR / OUTPUT_FILENAME.format(input_file.name)
    try:
        if output_file.exists():
            logging.info(f"Overwriting output file '{output_file}'")
        else:
            logging.info(f"Writing new output file '{output_file}'")
        
        with gzip.open(str(output_file), mode='wt') as gzf:
            for reward_key, records in rewarded_records.items():
                for record in records:
                    gzf.write((json.dumps(record) + "\n"))
    except Exception as e:
        logging.error(
            f'An error occurred while trying to write the gzipped file:'
            f'{str(output_file)}'
            f'{e}')
        raise e


def identify_dirs_to_process(input_dir, node_id: int, node_count: int):
    """
    Identify which dirs should this worker process.

    Args:
        input_dir : Path object towards the input folder.
        node_id   : int representing the id of the target node (zero-indexed)
        node_count: int representing the number of total nodes of the cluster
    
    Returns:
        List of Path objects representing folders
    """
    
    dirs_to_process = []
    for d in input_dir.iterdir():
        if d.is_dir():
            b64 = f'{d.name}=='
            directory_int = int.from_bytes(b64decode(b64), byteorder='little')
            if (directory_int % node_count) == node_id:
                dirs_to_process.append(d)
    
    return dirs_to_process


def worker():
    """
    Identify the relevant folders taht this worker should process, process 
    their files and write the gzipped results.
    
    """

    logging.info(f"Starting AWS Batch Array job.")

    node_id       = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])
    node_count    = int(os.environ['JOIN_REWARDS_JOB_ARRAY_SIZE'])
    reprocess_all = True if os.environ['JOIN_REWARDS_REPROCESS_ALL'].lower() == 'true' else False
    
    dirs_to_process = identify_dirs_to_process(PATH_INPUT_DIR, node_id, node_count)

    logging.debug(
        f"This instance (node {node_id}) will process the folders:"
        f"{', '.join([d.name for d in dirs_to_process])}"
    )
    
    # TODO:
    # Scan input directory and compare last modified times to the output 
    # directory. All updated input files are reprocessed by workers.

    # Output files without a corresponding input file will be deleted

    # Support a mode that will force deletion and reprocessing of all output files

    # Support graceful halting of the cluster since AWS Batch may halt it at any time.

    # While we make every effort to provide this warning as soon as possible, it is possible that your Spot Instance is terminated before the warning can be made available. Test your application to ensure that it handles an unexpected instance termination gracefully, even if you are testing for interruption notices. You can do so by running the application using an On-Demand Instance and then terminating the On-Demand Instance yourself. 

    if reprocess_all:
        pass

    for d in dirs_to_process:
        logging.debug(f"Processing folder '{d}'")
        files = [f for f in d.iterdir() if f.is_file()]
        logging.debug(f'Found {len(files)} files')
        for f in files:
            sorted_records = load_sorted_records(str(f))
            rewarded_records = assign_rewards_to_decisions(sorted_records)
            gzip_records(f, rewarded_records)
    logging.info("Finished.")


if __name__ == "__main__":
    worker()