# Built-in imports
import json
import re
import dateutil
import gzip
from collections.abc import Iterator
import zlib
import pathlib
import sys

# Local imports
import config
import utils
import src.train.constants as tc


MESSAGE_ID_KEY = 'message_id'
TIMESTAMP_KEY = 'timestamp'
TYPE_KEY = 'type'
DECISION_TYPE = 'decision'
REWARD_TYPE = 'reward'
EVENT_TYPE = 'event'
EVENT_KEY = EVENT_TYPE
MODEL_KEY = 'model'
REWARD_KEY = 'reward'
VARIANT_KEY = 'variant'
GIVENS_KEY = 'givens'
COUNT_KEY = 'count'
SAMPLE_KEY = 'sample'
RUNNERS_UP_KEY = 'runners_up'
PROPERTIES_KEY = 'properties'
VALUE_KEY = 'value'

class HistoryRecord:
    # slots are faster and use much less memory than dicts
    __slots__ = ['loaded_json_dict', MESSAGE_ID_KEY, TIMESTAMP_KEY, TYPE_KEY, MODEL_KEY, EVENT_KEY, PROPERTIES_KEY,
                 VALUE_KEY, REWARD_KEY, VARIANT_KEY, GIVENS_KEY, COUNT_KEY, RUNNERS_UP_KEY, SAMPLE_KEY]

    
    def __init__(self, json_dict: dict):
        assert isinstance(json_dict, dict)
        self.loaded_json_dict = json_dict
        self.reward = 0.0
        
        self.message_id = json_dict.get(MESSAGE_ID_KEY)

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
            
        self.event = json_dict.get(EVENT_KEY)

        self.properties = json_dict.get(PROPERTIES_KEY)
        
        self.value = 0.0
        if self.is_event_record():
            self.value = config.DEFAULT_EVENT_VALUE
            if isinstance(self.properties, dict):
                self.value = self.properties.get(VALUE_KEY, self.value)

        # all variant values are valid
        self.variant = json_dict.get(VARIANT_KEY)
        
        self.givens = json_dict.get(GIVENS_KEY)
        if not isinstance(self.givens, dict):
            self.givens = None
        
        self.count = json_dict.get(COUNT_KEY)
        if not isinstance(self.count, int):
            self.count = None
        
        # all sample values are valid
        self.sample = json_dict.get(SAMPLE_KEY)
        
        self.runners_up = json_dict.get(RUNNERS_UP_KEY)
        if not isinstance(self.runners_up, list):
            self.runners_up = None


    def is_valid(self):
        if self.message_id is None or self.timestamp is None or self.type is None:
            return False
        
        if self.is_decision_record():
            # variant may be None
            # sample may be None
            if self.model is None:
                return False
            if self.count is None or self.count < 1:
                return False

        return True


    def is_decision_record(self):
        return self.type == DECISION_TYPE
        
        
    def is_event_record(self):
        return self.type == EVENT_TYPE
        
        
    def is_reward_record(self):
        return self.type == REWARD_TYPE

        
    def reward_window_contains(self, other):
        return other.timestamp >= self.timestamp and other.timestamp <= self.timestamp + config.REWARD_WINDOW
            
        
    def to_rewarded_decision_dict(self):
        """ Return a dict representation of the decision record """
        
        result = {}
        
        result[TIMESTAMP_KEY] = self.timestamp.isoformat()
        result[REWARD_KEY] = self.reward
        result[VARIANT_KEY] = self.variant
        
        if self.givens is not None:
            result[GIVENS_KEY] = self.givens
        if self.count is not None:
            result[COUNT_KEY] = self.count
        if self.runners_up is not None:
            result[RUNNERS_UP_KEY] = self.runners_up
        if self.sample is not None:
            result[SAMPLE_KEY] = self.sample
            
        return result


def _load_records(file: pathlib.Path, message_ids: set) -> list:
    """
    Load records from a gzipped jsonlines file

    Parameters
    ----------
    file : path-like
        Path of the input gzipped jsonlines file to load
    message_ids : an empty set ?
        previously loaded message_ids to filter out in the event of 
        duplicates

    Returns
    -------
    list
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
                    record = HistoryRecord(json.loads(line))
                    
                    assert isinstance(record.message_id, str)
                    if record.message_id not in message_ids:
                        message_ids.add(record.message_id)
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


def _is_valid_model_name(model_name):   
    if not isinstance(model_name, str) \
            or len(model_name) == 0 \
            or not re.match(tc.MODEL_NAME_REGEXP, model_name):
        return False
        
    return True


def _all_valid_records(records):
    """ Check if all given records are valid.
    
    Parameters
    ----------
    records : list
        something here

    Returns
    -------
    bool
             
    """
    return len(records) == len(list(filter(lambda x: x.is_valid_record(), records)))