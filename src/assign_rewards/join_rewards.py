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
import json
import gzip
import io
import uuid
import sys
import shutil
import signal
import subprocess
import boto3
import botocore
from itertools import groupby
from collections import defaultdict

# Local imports
from utils import load_history
from utils import filter_valid_records
from utils import ensure_parent_dir
from config import DEFAULT_REWARD_KEY
from config import DEFAULT_EVENTS_REWARD_VALUE
from config import DEFAULT_REWARD_VALUE
from config import HISTORIES_PATH
from config import INCOMING_HISTORIES_PATH
from config import INCOMING_FIREHOSE_PATH
from config import LOGGING_LEVEL
from config import LOGGING_FORMAT
from config import REWARD_WINDOW
from config import AWS_BATCH_JOB_ARRAY_INDEX
from config import REWARD_ASSIGNMENT_WORKER_COUNT
from config import TRAIN_BUCKET

# stats constants
TOTAL_INCOMING_FILE_COUNT = "Incoming Files (Total)"
PROCESSED_INCOMING_FILE_COUNT = "Incoming Files Processed"
PROCESSED_HISTORY_FILE_COUNT = "History Files Processed"

# Time window to add to a timestamp
window = timedelta(seconds=REWARD_WINDOW)

SIGTERM = False

# boto3 client must be pre-initialized for multi-threaded (https://github.com/boto/botocore/issues/1246)
THREAD_WORKER_COUNT = 50
s3client = boto3.client("s3", config=botocore.config.Config(max_pool_connections=THREAD_WORKER_COUNT))


def process_incoming_history_file_group(file_group):
    handle_signals()

    stats = create_stats()

    # get the hashed history id
    hashed_history_id = hashed_history_id_from_file(file_group[0])
    
    # add any previously saved history files for this hashed history id
    file_group.extend(history_files_for_hashed_history_id(hashed_history_id, stats))

    # load all records
    records = load_history(file_group, stats)

    # write the consolidated records to a new history file
    save_history(hashed_history_id, records)
    
    # perform validation after consolidation so that invalid records are retained
    # this ensures that any bugs in user-supplied validation code doesn't cause records to be lost
    records = filter_valid_records(hashed_history_id, records)

    # assign rewards to decision records.
    rewarded_decisions_by_model = assign_rewards_to_decisions(records)
    
    # upload the updated rewarded decision records to S3
    for model, rewarded_decisions in rewarded_decisions_by_model.items():
        upload_rewarded_decisions(model, hashed_history_id, rewarded_decisions)
    
    # delete the incoming and history files that were processed
    delete_all(file_group)
    
    return stats

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

