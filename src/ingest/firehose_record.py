# Built-in imports
from __future__ import annotations
import datetime
import gzip
import math
from typing import List

# External imports
import orjson as json
import pandas as pd

# Local imports
from config import FIREHOSE_BUCKET, s3client, stats
from utils import utc, is_valid_model_name, is_valid_ksuid, get_valid_timestamp, \
    json_dumps_wrapping_primitive, json_dumps


MESSAGE_ID_KEY = 'message_id'
DECISION_ID_KEY = 'decision_id'
TIMESTAMP_KEY = 'timestamp'
TYPE_KEY = 'type'
DECISION_TYPE = 'decision'
REWARD_TYPE = 'reward'
MODEL_KEY = 'model'
REWARD_KEY = REWARD_TYPE
REWARDS_KEY = 'rewards'
VARIANT_KEY = 'variant'
GIVENS_KEY = 'givens'
COUNT_KEY = 'count'
SAMPLE_KEY = 'sample'
RUNNERS_UP_KEY = 'runners_up'

DF_SCHEMA = {
    DECISION_ID_KEY : 'object',
    TIMESTAMP_KEY   : 'datetime64[ns]',
    VARIANT_KEY     : 'object',
    GIVENS_KEY      : 'object',
    COUNT_KEY       : 'Int64',
    RUNNERS_UP_KEY  : 'object',
    SAMPLE_KEY      : 'object',
    REWARDS_KEY     : 'object',
    REWARD_KEY      : 'float64',
}

class FirehoseRecord:
    # slots are faster and use much less memory than dicts
    __slots__ = [MESSAGE_ID_KEY, TIMESTAMP_KEY, TYPE_KEY, MODEL_KEY, DECISION_ID_KEY, REWARD_KEY, VARIANT_KEY, GIVENS_KEY, COUNT_KEY, RUNNERS_UP_KEY, SAMPLE_KEY]
   
    # throws TypeError if json_record is wrong type
    # throws ValueError if record is invalid
    # throws KeyError if required field is missing
    def __init__(self, json_record):
        #
        # local variables are used in this method rather than self for performance
        #

        assert isinstance(json_record, dict)
        
        message_id = json_record[MESSAGE_ID_KEY]
        if not is_valid_ksuid(message_id):
            raise ValueError('invalid message_id')

        self.message_id = message_id
        
        # parse and validate timestamp
        timestamp = get_valid_timestamp(json_record[TIMESTAMP_KEY])
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=utc)
        
        self.timestamp = timestamp
        
        type_ = json_record[TYPE_KEY]
        if not isinstance(type_, str):
            raise ValueError('invalid type')
            
        self.type = type_
             
        model = json_record[MODEL_KEY]
        if not is_valid_model_name(model):
            raise ValueError('invalid model')
            
        self.model = model

        if self.is_reward_record():
            decision_id = json_record[DECISION_ID_KEY]
            if not is_valid_ksuid(decision_id):
                raise ValueError('invalid decision_id')
            self.decision_id = decision_id

            # parse and validate reward
            reward = json_record[REWARD_KEY]
    
            if not isinstance(reward, (int, float)):
                raise ValueError('invalid reward')
    
            self.reward = reward
            
            
        elif self.is_decision_record():
            # parse variant (all JSON types are valid)
            self.variant = json_record.get(VARIANT_KEY, None)
            
            # parse and validate given
            givens = json_record.get(GIVENS_KEY, None)
            
            if givens is not None and not isinstance(givens, dict):
                raise ValueError('invalid givens')
    
            self.givens = givens
            
            # parse and validate count
            count = json_record[COUNT_KEY]
    
            if not isinstance(count, int) or count < 1:
                raise ValueError('invalid count')
                
            self.count = count
    
            # parse and validate runners up
            runners_up = json_record.get(RUNNERS_UP_KEY, None)
    
            if runners_up is not None:
                if not isinstance(runners_up, list) or len(runners_up) == 0:
                    raise ValueError('invalid runners_up')
    
            self.runners_up = runners_up
    
            # parse and validate sample
            self.sample = json_record.get(SAMPLE_KEY, None)
            
            has_sample = False
            if SAMPLE_KEY in json_record:
                # null is a valid sample so we need a boolean to indicate presence
                has_sample = True
    
            # validate sample pool size
            sample_pool_size = _get_sample_pool_size(count, runners_up)
    
            if sample_pool_size < 0:
                raise ValueError('invalid count or runners_up')
    
            if has_sample:
                if sample_pool_size == 0:
                    raise ValueError('invalid count or runners_up')
            else:
                if sample_pool_size > 0:
                    raise ValueError('missing sample')


    def has_sample(self):
        # init validates that if and only if a sample is present (which can be null), the sample pool size is >= 1
        return self.sample_pool_size() >= 1
    

    def sample_pool_size(self):
        return _get_sample_pool_size(self.count, self.runners_up)
        

    def is_decision_record(self):
        return self.type == DECISION_TYPE
        
        
    def is_reward_record(self):
        return self.type == REWARD_TYPE

        
    def to_rewarded_decision_dict(self):
        """ Convert the firehose record to a dict representation of the rewarded decision record """
        
        result = {}
        
        if self.is_decision_record():
            # 'decision_id', 'timestamp', 'variant', 'givens', and 'count' must all be set
            # when converting from 'type' == 'decision' firehose records
            #
            # primitive values (including null values) are wrapped to ensure that all encoded JSON strings 
            # are either a JSON encoded dictionary or a JSON encoded list
            result[DECISION_ID_KEY] = self.message_id
            result[TIMESTAMP_KEY] = self.timestamp
            result[VARIANT_KEY] = json_dumps_wrapping_primitive(self.variant)
            result[GIVENS_KEY] = json_dumps_wrapping_primitive(self.givens)
            result[COUNT_KEY] = self.count
            
            # A not set runners_up must not be set in the result dictionary
            if self.runners_up is not None:
                # runners_up is always an array so don't wrap it
                result[RUNNERS_UP_KEY] = json_dumps(self.runners_up)
            
            # A sample may either be not set or may have a null value (or non-null value).
            # A set null sample must be wrapped and JSON encoded.
            # A not set sample must not be set in the result dictionary
            if self.has_sample():
                result[SAMPLE_KEY] = json_dumps_wrapping_primitive(self.sample)
                
        elif self.is_reward_record():
            # Only 'decision_id' and 'rewards' may be set when converting from 'type' == 'reward' firehose records
            # Do NOT copy the 'timestamp' field!
            result[DECISION_ID_KEY] = self.decision_id
            result[REWARDS_KEY] = json_dumps({ self.message_id: self.reward })

        return result

    def __str__(self):

        if self.is_decision_record():
            return f'message_id {self.message_id} '\
                   f'type {self.type} '\
                   f'model {self.model} '\
                   f'count {self.count} '\
                   f'givens {self.givens} '\
                   f'variant {self.variant} ' \
                   f'runners_up {self.runners_up} '\
                   f'sample {self.sample} '\
                   f'timestamp {self.timestamp}' 

        elif self.is_reward_record():
            return f'message_id {self.message_id} '\
                f'type {self.type} '\
                f'model {self.model} '\
                f'decision_id {self.decision_id} '\
                f'reward {self.reward}' \
                f'timestamp {self.timestamp}'
    

def _get_sample_pool_size(count, runners_up):
    sample_pool_size = count - 1 - (len(runners_up) if runners_up else 0)  # subtract chosen variant and runners up from count
    assert sample_pool_size >= 0
    return sample_pool_size


class FirehoseRecordGroup:


    def __init__(self, model_name, records: List[FirehoseRecord]):
        assert(is_valid_model_name(model_name))
        self.model_name = model_name
        self.records = records
        

    def to_rewarded_decision_dicts(self):
        assert(self.records)
        return list(map(lambda x: x.to_rewarded_decision_dict(), self.records))


    def to_pandas_df(self):
        df = pd.DataFrame(self.to_rewarded_decision_dicts(), columns=DF_SCHEMA.keys())
        df[TIMESTAMP_KEY] = pd.to_datetime(df[TIMESTAMP_KEY], utc=True).dt.tz_localize(None)
        return df.astype(DF_SCHEMA)
        

    @staticmethod
    def load_groups(s3_key) -> List[FirehoseRecordGroup]:
        assert(s3_key)
        """
        Load records from a gzipped jsonlines file
        """
        
        records_by_model = {}
        invalid_records = []
       
        print(f'loading s3://{FIREHOSE_BUCKET}/{s3_key}')
    
        # download and parse the firehose file
        s3obj = s3client.get_object(Bucket=FIREHOSE_BUCKET, Key=s3_key)['Body']
        stats.increment_s3_requests_count('get')

        with gzip.GzipFile(fileobj=s3obj) as gzf:
            for line in gzf.readlines():
    
                try:
                    record = FirehoseRecord(json.loads(line))
                    model = record.model
                    
                    if model not in records_by_model:
                        records_by_model[model] = []
                    
                    records_by_model[model].append(record)
                    stats.increment_valid_records_count()
                    
                except Exception as e:
                    stats.add_parse_exception(e)
                    invalid_records.append(line)
                    stats.increment_invalid_records_count()
                    continue
    
        if len(invalid_records):
            # TODO write invalid records to /unrecoverable
            pass
    
        total_records = stats.valid_records_count + stats.invalid_records_count

        print("JSONL has {} record(s): {} valid record(s) and {} invalid record(s)".format(
            total_records,
            stats.valid_records_count,
            stats.invalid_records_count
        ))
        
        results = []
        for model, records in records_by_model.items():
            results.append(FirehoseRecordGroup(model, records))
        
        print(f"Created {len(results)} FirehoseRecordGroup(s)")
        for except_str,count in stats.parse_exception_counts.items():
            print(f"Found {count} of the following exceptions: {except_str}")
        print("Finished loading FirehoseRecordGroup(s)")
        
        return results


def is_valid_type(x):
    return isinstance(x, str)


def is_valid_givens(x):
    return (x is None) or isinstance(x, dict)


def is_valid_count(x):
    return isinstance(x, int) and (x >= 1)


def naive_is_valid_runners_up(x):
    """ Naive because there is also other validation on runners up that 
    depends on the number of variants, not part of this function """
    if x is None:
        return True
    elif isinstance(x, list) and len(x) > 0:
        return True

    return False


def is_valid_reward(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


def is_valid_decision_id(x):
    return is_valid_ksuid(x)


def is_valid_message_id(x):
    return is_valid_ksuid(x)


def is_valid_sample(x):
    try:
        json.dumps(x)
        return True
    except:
        return False


def assert_valid_record(json_dict):
    assert isinstance(json_dict, dict), "record is not a dict"
    
    ##########################################################################
    # Common fields assertions
    ##########################################################################
    message_id = json_dict[MESSAGE_ID_KEY]
    assert is_valid_message_id(message_id), f"invalid message_id: {message_id}"
    
    timestamp = json_dict[TIMESTAMP_KEY]
    assert get_valid_timestamp(timestamp), f"invalid timestamp: {timestamp}"
    
    rec_type = json_dict[TYPE_KEY]
    assert is_valid_type(rec_type), f"invalid type: {rec_type}"

    model_name = json_dict[MODEL_KEY]
    assert is_valid_model_name(model_name), f"invalid model name: {model_name}"


    ##########################################################################
    # Decision record fields assertions
    ##########################################################################
    if json_dict[TYPE_KEY] == "decision":

        assert is_valid_givens(json_dict[GIVENS_KEY]), "invalid 'givens'"
        
        runners_up = json_dict.get(RUNNERS_UP_KEY, None)
        assert naive_is_valid_runners_up(runners_up), "invalid' runners up'"

        count = json_dict[COUNT_KEY]
        assert is_valid_count(count), f"invalid 'count': {count}"

        has_sample = SAMPLE_KEY in json_dict
        sample_pool_size = _get_sample_pool_size(count, runners_up)

        assert sample_pool_size >= 0, "invalid 'count' or 'runners up'"

        if has_sample:
            assert sample_pool_size > 0, "invalid 'count' or 'runners up'"
        else:
            assert sample_pool_size == 0, "missing sample"
    

    ##########################################################################
    # Reward record fields assertions
    ##########################################################################
    elif json_dict[TYPE_KEY] == "reward":

        assert is_valid_decision_id(json_dict[DECISION_ID_KEY]), \
            "invalid 'decision id'"

        assert is_valid_reward(json_dict[REWARD_KEY]), "invalid 'reward'"


def assert_valid_rewarded_decision_record(rdr_dict, record_type):
    """
    """

    assert record_type in ("reward", "decision")

    ##########################################################################
    # decision_id validation
    ##########################################################################
    assert is_valid_decision_id(rdr_dict[DECISION_ID_KEY]), \
        f"invalid decision_id: {rdr_dict[DECISION_ID_KEY]}"


    ##########################################################################
    # rewards validation
    ##########################################################################
    rewards = rdr_dict.get(REWARDS_KEY)
    
    if rewards is not None:

        assert isinstance(rewards, str),  \
            f"{REWARDS_KEY} must be a json str, got {type(rewards)}"

        rewards_dict = json.loads(rewards)
        assert isinstance(rewards_dict, dict), \
            f"the json str 'rewards' must contain a dict, got {type(rewards_dict)}"
        
        for key,val in rewards_dict.items():

            assert is_valid_ksuid(key), \
                f"invalid message_id of one reward in 'rewards': {key}"

            assert is_valid_reward(val), \
                f"invalid reward in 'rewards'"
    

    ##########################################################################
    # reward validation
    ##########################################################################
    reward = rdr_dict.get(REWARD_KEY)
    if reward is not None:
        assert is_valid_reward(reward), f"invalid {REWARD_KEY}"
        
        assert REWARDS_KEY in rdr_dict, \
            f"{REWARD_KEY} present but {REWARDS_KEY} is missing"
        
        assert isinstance(rewards, str),  \
            f"{REWARDS_KEY} must be a json str, got {type(rewards)}"
        
        rewards_dict = json.loads(rewards)
        assert isinstance(rewards_dict, dict), \
            f"the json str 'rewards' must contain a dict, got {type(rewards_dict)}"
        
        for key in rewards_dict.keys():
            assert is_valid_decision_id(key), "invalid 'decision id' in 'rewards'"

        assert reward == sum(rewards_dict.values()), \
            f"{REWARD_KEY} != sum({REWARDS_KEY})"
    

    if record_type == "decision":

        ######################################################################
        # timestamp validation
        ######################################################################
        # Can't do the following because the rewarded decision record returned
        # by to_rewarded_decision_dict is a datetime object
        # assert get_valid_timestamp(rdr_dict[TIMESTAMP_KEY]), "invalid timestamp"
        assert isinstance(rdr_dict[TIMESTAMP_KEY], datetime.datetime)


        ######################################################################
        # variant validation
        ######################################################################
        variant = rdr_dict[VARIANT_KEY]
        
        assert isinstance(variant, str), \
            f"'variant' must be a json string, got {type(variant)}"
        
        assert json.loads(variant) is not None, \
            "'variant' must be non-null: {variant}"


        ######################################################################
        # givens validation
        ######################################################################
        givens = rdr_dict[GIVENS_KEY]
        assert isinstance(givens, str), \
            f"'givens' must be a json str, got {type(givens)}"
        assert isinstance(json.loads(givens), dict), \
            f"the json str 'givens' must be a dict, got {type(json.loads(givens))}"


        ######################################################################
        # count validation
        ######################################################################
        count = rdr_dict[COUNT_KEY]
        assert is_valid_count(count), f"invalid 'count': {count}"


        ######################################################################
        # runners_up validation
        ######################################################################
        runners_up = rdr_dict.get(RUNNERS_UP_KEY)
        
        if runners_up is not None:
            
            assert isinstance(runners_up, str), "'runners_up' should be a str" 

            runners_up_list = json.loads(runners_up)
            assert isinstance(runners_up_list, list), \
                "'runners_up' must be a list"

            assert len(runners_up_list) > 0, "len(runners_up) must be > 0"
    
        # runners up must not be set if missing
        elif runners_up is None:
            assert RUNNERS_UP_KEY not in rdr_dict, \
                f"{RUNNERS_UP_KEY} must not be set if missing"


        ######################################################################
        # sample validation
        ######################################################################
        # sample must not be set if missing
        sample = rdr_dict.get(SAMPLE_KEY)
        if sample is None:
            assert SAMPLE_KEY not in rdr_dict, \
                f"{SAMPLE_KEY} must not be set if missing"
        
        else:
            assert isinstance(sample, str), f"'sample' must be a str: {sample}"
            assert is_valid_sample(json.loads(sample)), \
                f"invalid 'sample': {sample}"


    elif record_type == "reward":

        ######################################################################
        # timestamp validation: Timestamp must not be there
        ######################################################################
        assert rdr_dict.get(TIMESTAMP_KEY) is None, \
            "a partial rewarded decision record shouldn't have a timestamp"
        

        ######################################################################
        # variant validation
        ######################################################################
        variant = rdr_dict.get(VARIANT_KEY)
        assert variant is None , \
            (f"in a partial rewarded decision record, "
             f"'variant' must be None: {variant}")

