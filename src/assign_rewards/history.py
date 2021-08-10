import json
import re
import gzip
import hashlib
from uuid import uuid4
from itertools import groupby
import dateutil
from datetime import datetime
from datetime import timedelta
from operator import itemgetter

import constants
import config
import utils
import customize

LOWER_HEX_REGEXP = "^[a-f0-9]+$"
MODEL_NAME_REGEXP = "^[\w\- .]+$"
HISTORY_FILE_NAME_REGEXP = '^[\w]+$'
JSONLINES_FILENAME_EXTENSION_REGEXP = '.*\.jsonl.gz$'

SHA_256_LEN = 64
UUID4_LENGTH = 36

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
        assert not self.mutated # double check that we're not persisting modified records
        
        output_file = \
            utils.history_dir_for_hashed_history_id(
                self.hashed_history_id) / f'{self.hashed_history_id}-{uuid4()}.jsonl.gz'
    
        utils.ensure_parent_dir(output_file)
    
        # save all records, including invalid ones
        utils.save_gzipped_jsonlines(output_file.absolute(), self.records)
        
    def clean_up(self):
        # delete the incoming and history files that were processed
        utils.delete_all(self.files)
        
    def filter_valid_records(self):
        self.mutated = True
        
        results = []
        validated_history_id = None
    
        for record in self.records:
            if not is_valid_record(record, validated_history_id, self.hashed_history_id):
                continue
            
            results.append(record)
    
            if not validated_history_id:
                # history_id has been validated to hash to the hashed_history_id
                validated_history_id = record[constants.HISTORY_ID_KEY]
    
        self.records = results

        config.stats.incrementValidatedRecordCount(len(self.records))


    def sort_records(self):
        self.mutated = True
        # sort by timestamp. On tie, records of type 'decision' are sorted earlier
        self.records.sort(
            key=lambda x: (x[constants.TIMESTAMP_KEY], 0 if is_decision_record(x) else 1))

        
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
        decision_records = list(filter(lambda x: is_decision_record(x), self.records))

        # sort by model for groupby
        decision_records.sort(key = itemgetter(constants.MODEL_KEY))
        for model_name, rewarded_decisions in groupby(decision_records, itemgetter(constants.MODEL_KEY)):
    
            # filter out any keys that don't need to be used in training
            rewarded_decisions = map(copy_rewarded_decision_keys, rewarded_decisions)

            s3_key = rewarded_decisions_s3_key(model_name, self.hashed_history_id)
            
            utils.upload_gzipped_jsonlines(config.TRAIN_BUCKET, s3_key, rewarded_decisions)
            
            config.stats.addModel(model_name)
            config.stats.incrementRewardedDecisionCount(len(rewarded_decisions))
                

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
    

def select_incoming_history_files():
    # hash based on the first 8 characters of the hashed history id
    # return select_files_for_node(config.INCOMING_PATH, '*.jsonl.gz')
    # TODO check valid file name & hashed history id chars
    files_for_node = select_files_for_node(config.INCOMING_PATH, '*.jsonl.gz')
    return files_for_node


def select_files_for_node(input_dir, glob):
    files_to_process = []
    file_count = 0
    for f in input_dir.glob(glob):
        if not is_valid_history_filename(f.name):
            print(f'skipping bad file name {f.name}')
            # increment  bad file names stats
            # config.stats.incrementUnrecoverableFileCount()
            continue

        file_count += 1
        # convert first 8 hex chars (32 bit) to an int
        # the file name starts with a uuidv4
        # check if int mod node_count matches our node
        if (int(f.name[:8], 16) % config.NODE_COUNT) == config.NODE_ID:
            files_to_process.append(f)

    print(f'selected {len(files_to_process)} of {file_count} files from {input_dir}/{glob} to process')

    return files_to_process


def group_files_by_hashed_history_id(files):
    return groupby(sorted(files, key=hashed_history_id_from_file),hashed_history_id_from_file)


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


def is_valid_history_filename(checked_filename: str):
    """
    Checks:
     - if extension jsonl.gz is present
     - length of first 'chunk' of name split by '-' (sha256 check)
     - length of all remaining chunks (uuid4) check
     length and allows only for alnum characters in provided filename
    Desired name pattern looks as follows:
    00eda666cee662eef503f3ab4b7d6375ceda6549821265fa4a5081ce46d4cbb1-202342f0-ee10-40d5-9207-7ab785187f0a.jsonl.gz
    """

    # bad extension
    if not re.match(JSONLINES_FILENAME_EXTENSION_REGEXP, checked_filename):
        # Filename has no jsonl.gz suffix
        return False

    sha_filename_chunk = checked_filename.split('-')[0]

    if not is_valid_hashed_history_id(sha_filename_chunk):
        return False

    uuid4_filename_chunk = '-'.join(checked_filename.split('-')[1:]).split('.')[0]
    if not utils.is_valid_uuid(uuid4_filename_chunk):
        return False

    return True
    
def is_valid_hashed_history_id(hashed_history_id):
    # illegal chars or bad length of sha256 chunk
    if not isinstance(hashed_history_id, str) \
            or len(hashed_history_id) != SHA_256_LEN \
            or not re.match(LOWER_HEX_REGEXP, hashed_history_id):
        return False
        
def is_valid_model_name(model_name):
    if not isinstance(model_name, str) \
            or not re.match(MODEL_NAME_REGEXP, model_name):
        return False

def hash_history_id(history_id):
    return hashlib.sha256(history_id.encode()).hexdigest()


def is_decision_record(record):
    return record.get(constants.TYPE_KEY) == constants.DECISION_TYPE
    
def is_event_record(record):
    return record.get(constants.TYPE_KEY) == constants.EVENT_TYPE


def is_valid_record(record, validated_history_id, hashed_history_id):
    # assertions should have been guaranteed by load
    assert isinstance(record, dict)
    assert isinstance(record[constants.TIMESTAMP_KEY], datetime.datetime)

    history_id = record.get(constants.HISTORY_ID_KEY)
    if not isinstance(history_id, str):
        return False

    if validated_history_id:
        # validated history id provided, see if they match
        if validated_history_id != history_id:
            return False
    elif not utils.hash_history_id(history_id) == hashed_history_id:
        # Record`s history_id mismatches hashed_history_id
        return False
        
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

