import json
import dateutil
import re
import gzip

import config
import utils

MESSAGE_ID_KEY = 'message_id'
TIMESTAMP_KEY = 'timestamp'
TYPE_KEY = 'type'
DECISION_TYPE = 'decision'
EVENT_TYPE = 'event'
MODEL_KEY = 'model'
REWARD_KEY = 'reward'
VARIANT_KEY = 'variant'
GIVEN_KEY = 'given'
COUNT_KEY = 'count'
SAMPLE_KEY = 'sample'
RUNNERS_UP_KEY = 'runners_up'
PROPERTIES_KEY = 'properties'
VALUE_KEY = 'value'

MODEL_NAME_REGEXP = "^[\w\- .]+$"

class HistoryRecord:
    
    def __init__(self, json_dict: dict):
        assert isinstance(json_dict, dict)
        self.json_dict = json_dict
        
        self.message_id = json_dict.get(MESSAGE_ID_KEY)
        if not isinstance(self.message_id, str):
            self.message_id = None

        # parse the timestamp into a datetime
        try:
            self.timestamp = dateutil.parser.parse(json_dict.get(TIMESTAMP_KEY))
        except ValueError:
            pass
        
        self.type = json_dict.get(TYPE_KEY)
        if not isinstance(self.type, str):
            self.type = None
             
        self.model = json_dict.get(MODEL_KEY)
        if not _is_valid_model_name(self.model):
            self.model = None

        self.reward = json_dict.get(REWARD_KEY)
        if not isinstance(self.reward, (int, float)):
            self.reward = 0.0

        # all variant values are valid
        self.variant = json_dict.get(VARIANT_KEY)
        
        self.given = json_dict.get(GIVEN_KEY)
        if not isinstance(self.given, dict):
            self.givens = None

        self.count = json_dict.get(COUNT_KEY)
        if not isinstance(self.count, int):
            self.count = None
        
        # all sample values are valid
        self.sample = json_dict.get(SAMPLE_KEY)
        
        self.runners_up = json_dict.get(RUNNERS_UP_KEY)
        if not isinstance(self.runners_up, list):
            self.runners_up = None

        self.properties = json_dict.get(PROPERTIES_KEY)
        self.value = 0.0
        if self.is_event_record():
            self.value = config.DEFAULT_EVENT_VALUE
            if isinstance(self.properties, dict):
                self.value = self.properties.get(VALUE_KEY, self.value)
                

    def is_valid(self):
        if self.message_id is None or self.timestamp is None or self.type is None:
            return False
        
        if self.model:
            if not _is_valid_model_name(self.model):
                return False
        elif self.is_decision_record():
            return False

        return True


    def is_decision_record(self):
        return self.type == DECISION_TYPE
        
        
    def is_event_record(self):
        return self.type == EVENT_TYPE
        
        
    def reward_window_contains(self, other):
        return other.timestamp >= self.timestamp and other.timestamp <= self.timestamp + config.REWARD_WINDOW
        
    
    def retention_bonus_window_contains(self, other):
        pass
    
    
    def to_rewarded_decision_dict(self):

        # validate the final record, raise exception if invalid to fail job
        if not self.is_valid():
            raise ValueError('invalid rewarded decision record, likely due to a customize.py bug')
            
        result = {}
        
        result[TIMESTAMP_KEY] = self.timestamp.isoformat()
        result[REWARD_KEY] = self.reward
        result[VARIANT_KEY] = self.variant
        
        if self.given is not None:
            result[GIVEN_KEY] = self.given
        if self.count is not None:
            result[COUNT_KEY] = self.count
        if self.runners_up is not None:
            result[RUNNERS_UP_KEY] = self.runners_up
        if self.sample is not None:
            result[SAMPLE_KEY] = self.sample


def _is_valid_model_name(model_name):
    if not isinstance(model_name, str) \
            or len(model_name) == 0 \
            or not re.match(MODEL_NAME_REGEXP, model_name):
        return False
        
    return True

def _all_valid_records(records):
    return len(records) == len(list(filter(lambda x: x.is_valid_record(), records)))

def _load_records(file, message_ids: set) -> list:
    """
    Load records from a gzipped jsonlines file

    Args:
        file: Path of the input gzipped jsonlines file to load
        message_ids: previously loaded message_ids to filter out
        in the event of duplicates

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

                    message_id = record[MESSAGE_ID_KEY]
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
