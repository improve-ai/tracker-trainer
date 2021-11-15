# Built-in imports
import orjson as json
import re
import dateutil
import datetime
import gzip
from ksuid import Ksuid
import math

# Local imports
from config import s3client, FIREHOSE_BUCKET, stats
from utils import utc, is_valid_model_name, is_valid_ksuid, get_valid_timestamp, json_dumps_wrapping_primitive, json_dumps


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
            # only 'decision_id' and 'rewards' may be set when converting from 'type' == 'reward' firehose records
            # do NOT copy the 'timestamp' field!
            result[DECISION_ID_KEY] = self.decision_id
            result[REWARDS_KEY] = json_dumps({ self.message_id: self.reward })

        return result


    def __str__(self):
        return f'message_id {self.message_id} type {self.type} model {self.model} decision_id {self.decision_id}' \
        f'reward {self.reward} count {self.count} givens {self.givens} variant {self.variant}' \
        f' runners_up {self.runners_up} sample {self.sample} timestamp {self.timestamp}' 
    

def _get_sample_pool_size(count, runners_up):
    sample_pool_size = count - 1 - (len(runners_up) if runners_up else 0)  # subtract chosen variant and runners up from count
    assert sample_pool_size >= 0
    return sample_pool_size


class FirehoseRecordGroup:
    
    
    def __init__(self, model_name, records):
        assert(is_valid_model_name(model_name))
        self.model_name = model_name
        self.records = records
        

    def to_rewarded_decision_dicts(self):
        assert(self.records)
        return list(map(lambda x: x.to_rewarded_decision_dict(), self.records))


    @staticmethod
    def load_groups(s3_key):
        assert(s3_key)
        """
        Load records from a gzipped jsonlines file
        """
        
        records_by_model = {}
        invalid_records = []
        
        print(f'loading s3://{FIREHOSE_BUCKET}/{s3_key}')
    
        # download and parse the firehose file
        s3obj = s3client.get_object(Bucket=FIREHOSE_BUCKET, Key=s3_key)['Body']
        with gzip.GzipFile(fileobj=s3obj) as gzf:
            for line in gzf.readlines():
    
                try:
                    record = FirehoseRecord(json.loads(line))
                    model = record.model
                    
                    if model not in records_by_model:
                        records_by_model[model] = []
                    
                    records_by_model[model].append(record)
                    
                except Exception as e:
                    stats.add_parse_exception(e)
                    invalid_records.append(line)
                    continue
    
        if len(invalid_records):
            print(f'skipped {len(invalid_records)} invalid records')
            # TODO write invalid records to /uncrecoverable
    
        print(f'loaded {sum(map(len, records_by_model.values()))} records from firehose')
        
        results = []
        for model, records in records_by_model.items():
            results.append(FirehoseRecordGroup(model, records))
            
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

        assert is_valid_givens(json_dict[GIVENS_KEY]), "invalid givens"
        
        runners_up = json_dict.get(RUNNERS_UP_KEY, None)
        assert naive_is_valid_runners_up(runners_up), "invalid runners up"

        count = json_dict[COUNT_KEY]
        assert is_valid_count(count), f"invalid count: {count}"

        has_sample = SAMPLE_KEY in json_dict
        sample_pool_size = _get_sample_pool_size(count, runners_up)

        assert sample_pool_size >= 0, "invalid count or runners_up"

        if has_sample:
            assert sample_pool_size > 0, "invalid count or runners_up"
        else:
            assert sample_pool_size == 0, "missing sample"
    

    ##########################################################################
    # Reward record fields assertions
    ##########################################################################
    elif json_dict[TYPE_KEY] == "reward":

        assert is_valid_decision_id(json_dict[DECISION_ID_KEY]), "invalid decision_id"

        assert is_valid_reward(json_dict[REWARD_KEY]), "invalid reward"


def assert_valid_rewarded_decision_record(record_dict, record_type):
    """
    The fields' validations here are slightly different than the ones for the normal records
    """

    ##########################################################################
    # decision_id validation
    ##########################################################################
    assert is_valid_decision_id(record_dict[DECISION_ID_KEY]), "invalid decision_id"


    ##########################################################################
    # rewards validation
    ##########################################################################
    rewards = record_dict.get(REWARDS_KEY)
    
    if rewards is not None:
        
        # The actual "rewards" is wrapped in a list so that Pandas can ingest it 
        rewards = rewards[0]

        assert isinstance(rewards, dict),  "rewards must be a dict or missing/null"
        
        for i in rewards.values():
            assert is_valid_reward(i), "invalid 'reward' inside 'rewards'"
    
        for key in rewards.keys():
            assert is_valid_ksuid(key), "invalid message_id of one 'reward' inside 'rewards'"


    ##########################################################################
    # reward validation
    ##########################################################################
    reward = record_dict.get(REWARD_KEY)
    if reward is not None:
        assert is_valid_reward(reward), "invalid reward"
        if rewards is not None:
            assert reward == sum(rewards.values()), "reward != sum(rewards)"


    if record_type == "decision":

        ######################################################################
        # timestamp validation
        ######################################################################
        # Can't do the following because the rewarded decision record returned
        # by to_rewarded_decision_dict is a datetime object
        # assert get_valid_timestamp(record_dict[TIMESTAMP_KEY]), "invalid timestamp"
        assert isinstance(record_dict[TIMESTAMP_KEY], datetime.datetime)


        ######################################################################
        # variant validation
        ######################################################################
        variant = record_dict[VARIANT_KEY]
        
        assert isinstance(variant, str), \
            (f"in a rewarded decision record, "
            "variant must be a json string: {variant}")
        
        assert json.loads(variant) is not None, \
            (f"in a rewarded decision record, "
            "variant must be non-null: {variant}")


        ######################################################################
        # givens validation
        ######################################################################
        givens = record_dict[GIVENS_KEY]
        assert isinstance(givens, str), \
            "in a rewarded decision record, givens must be a json string"
        assert isinstance(json.loads(givens), dict), "givens must be a dict"


        ######################################################################
        # count validation
        ######################################################################
        count = record_dict[COUNT_KEY]
        assert is_valid_count(count), f"invalid count: {count}"


        ######################################################################
        # runners_up validation
        ######################################################################
        runners_up = record_dict.get(RUNNERS_UP_KEY)
        if runners_up is not None:
            assert isinstance(runners_up, list)
            for i in runners_up:
                assert isinstance(i, str)
                assert json.loads(i)
        # runners up must not be set if missing
        elif runners_up is None:
            assert RUNNERS_UP_KEY not in record_dict


        ######################################################################
        # sample validation
        ######################################################################
        # sample must not be set if missing
        sample = record_dict.get(SAMPLE_KEY)
        if sample is None:
            assert SAMPLE_KEY not in record_dict
        else:
            assert isinstance(sample, str)
            assert is_valid_sample(json.loads(sample))


    elif record_type == "reward":

        ######################################################################
        # timestamp validation: Timestamp must not be there
        ######################################################################
        assert record_dict.get(TIMESTAMP_KEY) is None, \
            "a partial rewarded decision record shouldn't have a timestamp"
        

        ######################################################################
        # variant validation
        ######################################################################
        variant = record_dict.get(VARIANT_KEY)
        assert variant == "null" , \
            (f"in a partial rewarded decision record, "
             f"variant must be the json str 'null': {variant}")