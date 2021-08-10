import os
import json
import re
import gzip
import hashlib
from uuid import uuid4
from itertools import groupby
import dateutil
from datetime import datetime
from operator import itemgetter

import constants
import config
import utils
import customize

HASHED_HISTORY_ID_REGEXP = "^[a-f0-9]+$"
MODEL_NAME_REGEXP = "^[\w\- .]+$"

HASHED_HISTORY_ID_LEN = 64

class History:
    
    def __init__(self, hashed_history_id, files):
        # get the hashed history id
        self.hashed_history_id = hashed_history_id
        self.files = files

        
    def process(self):

        # load all records, ignoring subsequent records with duplicate message_ids
        self.load()
    
        # write the consolidated records to a new history file
        # save prior to validation so that invalid records are retained
        # save prior to customization and reward assignment since those will mutate records
        self.save()
    
        # perform deeper validation before reward assignement
        self.filter_valid_records()

        self.before_assign_rewards()
    
        # assign rewards to decision records.
        self.assign_rewards()
    
        self.after_assign_rewards()
    
        self.upload_rewarded_decisions()
    
        # delete processed incoming and history files.
        # do this last in case there is a problem during processing that needs to be retried
        self.clean_up()
    
    
    def load(self):
        self.records = []
        self.mutated = False

        message_ids = set()
    
        # iterate the files. later records with duplicate message ids will be ignored
        for file in self.files:
            self.records.extend(load_records(file, message_ids))
    

    def save(self):
        assert not self.mutated # double check that we're not persisting modified or filtered records
        
        output_file = history_dir_for_hashed_history_id(self.hashed_history_id) / f'{self.hashed_history_id}-{uuid4()}.jsonl.gz'
    
        utils.ensure_parent_dir(output_file)
    
        # save all records
        utils.save_gzipped_jsonlines(output_file.absolute(), self.records)
        
        
    def clean_up(self):
        # delete the incoming and history files that were processed
        utils.delete_all(self.files)
        
        
    def filter_valid_records(self):
        self.mutated = True
        
        self.records = list(filter(is_valid_record, self.records))
        
        config.stats.incrementValidatedRecordCount(len(self.records))


    def sort_records(self):
        self.mutated = True
        # sort by timestamp. On tie, records of type 'decision' are sorted earlier
        self.records.sort(key=lambda x: (x[constants.TIMESTAMP_KEY], 0 if is_decision_record(x) else 1))

        
    def before_assign_rewards(self):
        self.mutated = True
        self.sort_records()
        self.records = customize.before_assign_rewards(self.records)

        
    def after_assign_rewards(self):
        self.mutated = True
        self.sort_records()
        self.records = customize.after_assign_rewards(self.records)

        
    def assign_rewards(self):
        self.mutated = True
        self.sort_records()
        rewardable_decisions = []
    
        for record in self.records:
            if is_decision_record(record):
                rewardable_decisions.append(record)
            elif is_event_record(record):
                reward = record.get(constants.PROPERTIES_KEY, {}).get(constants.VALUE_KEY, config.DEFAULT_EVENT_VALUE)
                # Loop backwards to be able to remove an item in place
                for i in range(len(rewardable_decisions) - 1, -1, -1):
                    listener = rewardable_decisions[i]
                    if listener[constants.TIMESTAMP_KEY] + config.REWARD_WINDOW < record[constants.TIMESTAMP_KEY]:
                        del rewardable_decisions[i]
                    else:
                        listener[constants.REWARD_KEY] = listener.get(constants.REWARD_KEY, 0.0) + float(reward)

    
    def upload_rewarded_decisions(self):
        # extract decision records
        decision_records = list(filter(is_decision_record, self.records))
        
        # validate the final decision records, raise exception if invalid to fail job
        if not all_valid_records(decision_records):
            raise ValueError('invalid rewarded decision records found prior to upload')
        
        # sort by model for groupby
        decision_records.sort(key = itemgetter(constants.MODEL_KEY))
        for model_name, rewarded_decisions in groupby(decision_records, itemgetter(constants.MODEL_KEY)):
    
            # filter out any keys that don't need to be used in training
            rewarded_decisions = list(map(copy_rewarded_decision_keys, rewarded_decisions))

            s3_key = rewarded_decisions_s3_key(model_name, self.hashed_history_id)
            
            utils.upload_gzipped_jsonlines(config.TRAIN_BUCKET, s3_key, rewarded_decisions)
            
            config.stats.addModel(model_name)
            config.stats.incrementRewardedDecisionCount(len(rewarded_decisions))
                
    
def histories_to_process():
    histories = []
    
    # identify the portion of incoming history files to process in this node
    incoming_history_files = select_incoming_history_files_for_node()
    for hashed_history_id, incoming_history_file_group in \
            groupby(sorted(incoming_history_files, key=hashed_history_id_from_file), hashed_history_id_from_file):
    
        files = list(incoming_history_file_group)
        
        # sort the newest incoming files to the beginning of the list so that they get
        # precedence for duplicate message ids
        files.sort(key=os.path.getctime, reverse=True)

        # list the previously saved history files for this hashed_history_id
        history_files = history_files_for_hashed_history_id(hashed_history_id)

        # independently sort the newest history files to the beginning of their list so that they get
        # precedence for duplicate message ids
        history_files.sort(key=os.path.getctime, reverse=True)
        
        # add any previously saved history files for this hashed history id to
        # the end of the file group. In the event of duplicate message_ids,
        # the incoming history files will take precedence because they are
        # earlier in the file group. 
        files.extend(history_files)
    
        histories.append(History(hashed_history_id, files))

    return histories


def load_records(file, message_ids):
    """
    Load a gzipped jsonlines file

    Args:
        file: Path of the input gzipped jsonlines file to load

    Returns:
        A list of records
    """

    line_count = 0

    records = []
    error = None

    try:
        with gzip.open(file.absolute(), mode="rt", encoding="utf8") as gzf:
            for line in gzf.readlines():
                line_count += 1  # count every line as a record whether it's parseable or not
                # Do a inner try/except to try to recover as many records as possible
                try:
                    record = json.loads(line)
                    # parse the timestamp into a datetime since it will be used often
                    record[constants.TIMESTAMP_KEY] = \
                        dateutil.parser.parse(record[constants.TIMESTAMP_KEY])

                    message_id = record[constants.MESSAGE_ID_KEY]
                    assert isinstance(message_id, str)
                    if message_id not in message_ids:
                        message_ids.add(message_id)
                        records.append(record)
                    else:
                        config.stats.incrementDuplicateMessageIdCount()
                except (json.decoder.JSONDecodeError, ValueError, AssertionError) as e:
                    error = e
    except (zlib.error, EOFError, gzip.BadGzipFile) as e:
        # gzip can throw zlib.error, EOFError, or gzip.BadGZipFile on corrupt file
        error = e

    if error:
        # Unrecoverable parse error, copy the file to /unrecoverable
        dest = config.UNRECOVERABLE_PATH / file.name
        print(
            f'unrecoverable parse error "{error}", copying {file.absolute()} to {dest.absolute()}')
        utils.copy_file(file, dest)
        config.stats.incrementUnrecoverableFileCount()

    config.stats.incrementHistoryRecordCount(line_count)

    return records

def select_incoming_history_files_for_node():
    files = []
    file_count = 0
    glob = '*.jsonl.gz'
    for f in config.INCOMING_PATH.glob(glob):
        if not is_valid_hashed_history_id(hashed_history_id_from_file(f)):
            print(f'skipping bad file name {f}')
            config.stats.incrementBadFileNameCount()
            continue

        file_count += 1
        # convert first 8 hex chars (32 bit) to an int
        # the file name starts with the hashed history id
        # check if int mod node_count matches our node
        if (int(f.name[:8], 16) % config.NODE_COUNT) == config.NODE_ID:
            files.append(f)

    print(f'selected {len(files)} of {file_count} files from {config.INCOMING_PATH}/{glob} to process')

    return files


def unique_hashed_history_file_name(hashed_history_id):
    return f'{hashed_history_id}-{uuid4()}.jsonl.gz'


def hashed_history_id_from_file(file):
    return file.name.split('-')[0]


def history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return config.HISTORIES_PATH / hashed_history_id[0:2] / hashed_history_id[2:4]


def history_files_for_hashed_history_id(hashed_history_id):
    results = \
        list(
            history_dir_for_hashed_history_id(hashed_history_id)
                .glob(f'{hashed_history_id}-*.jsonl.gz'))
    return results

def is_valid_hashed_history_id(hashed_history_id):
    # illegal chars or bad length of sha256 chunk
    if not isinstance(hashed_history_id, str) \
            or len(hashed_history_id) != HASHED_HISTORY_ID_LEN \
            or not re.match(HASHED_HISTORY_ID_REGEXP, hashed_history_id):
        return False
        
    return True
        
def is_valid_model_name(model_name):
    if not isinstance(model_name, str) \
            or not re.match(MODEL_NAME_REGEXP, model_name):
        return False
        
    return True

def hash_history_id(history_id):
    return hashlib.sha256(history_id.encode()).hexdigest()


def is_decision_record(record):
    return record.get(constants.TYPE_KEY) == constants.DECISION_TYPE
    
def is_event_record(record):
    return record.get(constants.TYPE_KEY) == constants.EVENT_TYPE

def all_valid_records(records):
    return len(records) == len(list(filter(is_valid_record, records)))

def is_valid_record(record):
    # assertions should have been guaranteed by load
    assert isinstance(record, dict)
    assert isinstance(record[constants.TIMESTAMP_KEY], datetime)

    _type = record.get(constants.TYPE_KEY)
    if not isinstance(_type, str):
        return False

    # 'decision' type requires a 'model'
    if _type == constants.DECISION_TYPE:
        if not is_valid_model_name(record.get(constants.MODEL_KEY)):
            return False

    if _type == constants.EVENT_TYPE:
        # ensure any properties are a dict
        properties = record.get(constants.PROPERTIES_KEY)
        if properties and not isinstance(properties, dict):
            return False
        
        # ensure any properties.value is a number
        value = properties.get(constants.VALUE_KEY)
        if value and not isinstance(value, (int, float)):
            return False
            
    # ensure any givens are a dict
    givens = record.get(constants.GIVEN_KEY)
    if givens and not isinstance(givens, dict):
        return False
        
    # ensure any existing reward is a number
    reward = record.get(constants.REWARD_KEY)
    if reward and not isinstance(reward, (int, float)):
        return False

    return True
    

def rewarded_decisions_s3_key(model, hashed_history_id):
    assert is_valid_model_name(model)
    assert is_valid_hashed_history_id(hashed_history_id)

    return f'rewarded_decisions/{model}/{hashed_history_id[0:2]}/{hashed_history_id[2:4]}/{hashed_history_id}.jsonl.gz'

def copy_rewarded_decision_keys(record):
    return {k: record[k] for k in record if k in constants.REWARDED_DECISION_KEYS}

