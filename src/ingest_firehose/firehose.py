# Built-in imports
import orjson as json
import re
import dateutil
import gzip

# Local imports
from config import s3client
import utils
import src.train.constants as tc


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
    
    def __init__(self, json_dict: dict):
        assert isinstance(json_dict, dict)

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

        self.decision_id = json_dict.get(DECISION_ID_KEY)
            
        # parse and validate reward
        reward = json_dict.get(REWARD_KEY, 0)

        if not isinstance(reward, (int, float)):
            raise ValueError('invalid reward')

        self.reward = reward

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
        
        
    def is_reward_record(self):
        return self.type == REWARD_TYPE

        
    def to_rewarded_decision_dict(self):
        """ Return a dict representation of the rewarded decision record """
        
        result = {}
        
        if self.is_decision_record():
            result[DECISION_ID_KEY] = self.message_id
            result[TIMESTAMP_KEY] = self.timestamp
            result[VARIANT_KEY] = json.dumps(self.variant)
            
            if self.givens is not None:
                result[GIVENS_KEY] = json.dumps(self.givens)
                
            if self.count is not None:
                result[COUNT_KEY] = self.count
                
            if self.runners_up is not None:
                result_runners_up = []
                for runner_up in self.runners_up:
                    result_runners_up.append(json.dumps(runner_up))
                result[RUNNERS_UP_KEY] = result_runners_up
                
            if self.sample is not None:
                result[SAMPLE_KEY] = json.dumps(self.sample)
                
        elif self.is_reward_record():
            # do NOT copy timestamp for reward record
            result[DECISION_ID_KEY] = self.decision_id
            result[REWARDS_KEY] = { self.message_id: self.reward }
            
        return result



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
    


def load_records(s3_bucket: str, s3_key: str) -> list:
    """
    Load records from a gzipped jsonlines file
    """
    
    records = []
    invalid_records = []
    
    print(f'loading s3://{s3_bucket}/{s3_key}')

    # download and parse the firehose file
    s3obj = s3client.get_object(s3_bucket, s3_key)['Body']
    with gzip.GzipFile(fileobj=s3obj) as gzf:
        for line in gzf.readlines():

            try:
                records.append(FirehoseRecord(json.loads(line)))
            except Exception as exc:
                invalid_records.append(line)
                continue

    if len(invalid_records):
        print(f'skipped {len(invalid_records)} invalid records')
        # TODO write invalid records to /uncrecoverable

    print(f'loaded {len(records)} records from firehose')
    
    return records
    

def rewarded_decisions_s3_key(model, hashed_history_id):
    assert _is_valid_model_name(model)
    assert is_valid_hashed_history_id(hashed_history_id)

    return f'rewarded_decisions/{model}/parq/{hashed_history_id[0:2]}/{hashed_history_id[2:4]}/{hashed_history_id}.parq'
