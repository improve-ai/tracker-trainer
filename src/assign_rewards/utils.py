# Built-in imports
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import logging
import json
import sys

# Local imports
from config import DATETIME_FORMAT


def load_records(filename):
    """
    Load a jsonlines file
    
    Args:
        filename: name of the input jsonlines file to load
    
    Returns:
        Three lists:
            - decision records
            - reward records
            - event records
    """

    decision_records = []
    reward_records = []
    event_records =[]

    with open(filename, encoding="utf8") as f:
        records = []
        for line in f.readlines():
            try:
                record = json.loads(line)
                if record['type'] == "decision":
                    decision_records.append(record)
                elif record['type'] == "rewards":
                    reward_records.append(record)
                elif record['type'] == "event":
                    event_records.append(record)
            except json.decoder.JSONDecodeError as e:
                logging.error(
                    f"Error reading a record in file '{filename}', skipping."
                    f"{e}")

    return decision_records, reward_records, event_records


def sort_records_by_timestamp(records):
    def sort_key(x):
        timestamp = datetime.strptime(x['timestamp'], DATETIME_FORMAT)        
        if x['type'] in ('rewards', 'event'):
            timestamp += timedelta(seconds=1)
        return timestamp
    
    records.sort(key=sort_key)

    return records


def name_no_ext(p):
    """Given a Path object or a str, return a filename without extensions"""
    if isinstance(p, Path):
        return p.stem.split('.')[0]
    if isinstance(p, str):
        return p.split(".")[0]


def deepcopy(o):
    return json.loads(json.dumps(o))