"""
This file is not needed by the unit tests. It is just a way of creating fake 
data for manual testing. It can be safely deleted since it's not being called
or used anywhere.

Create a folder structure like:
    /aa/aaMXdMaWdUNlFkZ1VLblhrd0JKM05iUGNkV1V2b2xv.jsonl
    /aa/aaZHJGWE1ReTl4STNxTURjSDBGM1BqeER2TjVBdWZm.jsonl
    ...
    /ab/abWU1WV0QxN3VqbmFhbkIyaUFrNEluTk5yMFVsNEhG.jsonl
    ...

Each file is a jsonl with decision records and rewards.
"""
# Local imports
from datetime import timedelta
from datetime import datetime
from base64 import b64encode
from pathlib import Path
import random
import string
import json

# External imports
import pytz

# Local imports
from config import DATETIME_FORMAT
from config import HISTORIES_PATH


RECORD_TYPES = ['decision', 'rewards']
REWARD_KEYS =  ['reward_key_a', 'reward_key_b', 'reward_key_c']
MAX_NUM_REWARDS = 100
MAX_NUM_DECISION_RECORDS = 200
MAX_NUM_FILES_PER_SUBFOLDER = 200
SUBDIRS = [
    'aa',
    'ab',
    'ac',
    'fs',
    'oe',
    'pe',
    'ZZ',
]


def random_b64_str(size=30):
    chars = string.ascii_letters + string.digits
    random_str = "".join(random.choices(chars, k=size))
    return b64encode(random_str.encode('ascii')).decode("utf-8")


def create_decision_record(history_id):
    timestamp = datetime.now(tz=pytz.utc) + timedelta(seconds=random.randint(0,100))

    decision_record = {
        "type": "decision",
        "reward_key": random.choice(REWARD_KEYS),
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
        "history_id": history_id
    }

    return decision_record


def create_reward_record(decision_record):
    str_timestamp = decision_record.get('timestamp')
    timestamp = datetime.strptime(str_timestamp, DATETIME_FORMAT)
    timestamp = timestamp + timedelta(seconds=random.randint(0,3600*2))

    reward_record = {
        "type" : "rewards",
        "timestamp": timestamp.strftime(DATETIME_FORMAT),
        "rewards": {
            decision_record.get('reward_key') : random.randint(0,1)
        },
        "history_id": decision_record.get('history_id')
    }

    return reward_record


def create_records(history_id, num_decision_records=None):
    if not num_decision_records:
        num_decision_records = random.randint(1, MAX_NUM_DECISION_RECORDS)

    records = []
    for i in range(num_decision_records):
        decision_record = create_decision_record(history_id)
        records.append(decision_record)
        num_rewards = random.randint(0, MAX_NUM_REWARDS)
        for j in range(num_rewards):
            records.append(create_reward_record(decision_record))

    return records


def create_jsonl_from_records(filepath, records):
    """
    Create a jsonl file with the given records.
    """
    filename = f'{filepath}.jsonl'
    with open(filename, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    for top_level in SUBDIRS:
        subdir = HISTORIES_PATH / top_level
        subdir.mkdir(parents=True, exist_ok=True)
        num_files = random.randint(1, MAX_NUM_FILES_PER_SUBFOLDER)
        for i in range(num_files):
            history_id = top_level + random_b64_str(30)
            records = create_records(history_id)
            create_jsonl_from_records(str(subdir/history_id), records)
