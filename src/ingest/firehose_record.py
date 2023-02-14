# Built-in imports
from __future__ import annotations
import gzip
from typing import List

# External imports
import orjson
import pandas as pd

# Local imports
from config import FIREHOSE_BUCKET, s3client
from utils import is_valid_model_name, is_valid_ksuid, json_dumps


MESSAGE_ID_KEY = 'message_id'
DECISION_ID_KEY = 'decision_id'
MODEL_KEY = 'model'
REWARD_KEY = 'reward'
REWARDS_KEY = 'rewards'
ITEM_KEY = 'item'
CONTEXT_KEY = 'context'
COUNT_KEY = 'count'
SAMPLE_KEY = 'sample'

# Use float64 instead of Int64. It will be loaded into an object
# dtype by default anyway and casting it to Int64 will always truncate (if count was float)
# and never raise. The only property of Int64 we were using was that is supports missing values
# (while int64 does not). However float64 supports missings as well and does not impact
# speed and makes code simpler as well
NUMERIC_COLUMNS_DTYPE = 'float64'

DF_SCHEMA = {
    DECISION_ID_KEY : 'object',
    ITEM_KEY     : 'object',
    CONTEXT_KEY      : 'object',
    COUNT_KEY       : NUMERIC_COLUMNS_DTYPE,
    SAMPLE_KEY      : 'object',
    REWARDS_KEY     : 'object',
    REWARD_KEY      : NUMERIC_COLUMNS_DTYPE,
}

DF_COLUMNS = list(DF_SCHEMA.keys())
EMPTY_REWARDS_JSON_ENCODED = '{}'
NO_REWARDS_REWARD_VALUE = 0.0

DECISION_ID_COLUMN_INDEX = 0
REWARDS_COLUMN_INDEX = -2
REWARD_COLUMN_INDEX = -1

class FirehoseRecord:
    # slots are faster and use much less memory than dicts
    __slots__ = [MESSAGE_ID_KEY, MODEL_KEY, DECISION_ID_KEY, REWARD_KEY, ITEM_KEY, CONTEXT_KEY, COUNT_KEY, SAMPLE_KEY, 'has_sample']
   
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
             
        model = json_record[MODEL_KEY]
        if not is_valid_model_name(model):
            raise ValueError('invalid model')
            
        self.model = model

        # parse and validate count
        # use the presence of count to determine if it is a decision or reward record
        count = json_record.get(COUNT_KEY, None)

        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('invalid count')
                
            self.count = count


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
            
            
        else: # decision record

            # parse variant (all JSON types are valid)
            self.item = json_record.get(ITEM_KEY, None)
            
            # parse and validate context (all JSON types are valid)
            self.context = json_record.get(CONTEXT_KEY, None)
    
            # parse and validate sample
            self.sample = json_record.get(SAMPLE_KEY, None)
            
            has_sample = False
            if SAMPLE_KEY in json_record:
                # null is a valid sample so we need a boolean to indicate presence
                has_sample = True

            if has_sample and count == 1:
                raise ValueError('invalid count of 1 with sample')
            
            self.has_sample = has_sample
            

    def is_decision_record(self):
        return hasattr(self, 'count') and self.count >= 1
        
        
    def is_reward_record(self):
        return not self.is_decision_record()

        
    def to_rewarded_decision_dict(self):
        """ Convert the firehose record to a dict representation of the rewarded decision record """
        
        result = {}
        
        if self.is_decision_record():
            # 'decision_id', 'item', 'context', and 'count' must all be set
            # when converting from decision type firehose records
            result[DECISION_ID_KEY] = self.message_id
            result[ITEM_KEY] = json_dumps(self.item)  
            result[CONTEXT_KEY] = json_dumps(self.context)
            result[COUNT_KEY] = self.count
                        
            # A sample may either be not set or may have a null value (or non-null value).
            # A set null sample must be JSON encoded.
            # A not set sample must not be set in the result dictionary
            if self.has_sample:
                result[SAMPLE_KEY] = json_dumps(self.sample)
                
        else: # reward record

            # Only 'decision_id' and 'rewards' may be set when converting from reward type firehose records
            result[DECISION_ID_KEY] = self.decision_id
            result[REWARDS_KEY] = json_dumps({self.message_id: self.reward})

        return result

    def __str__(self):

        if self.is_decision_record():
            return f'message_id {self.message_id} '\
                   f'model {self.model} '\
                   f'count {self.count} '\
                   f'context {self.context} '\
                   f'item {self.item} ' \
                   f'sample {self.sample} '

        elif self.is_reward_record():
            return f'message_id {self.message_id} '\
                f'model {self.model} '\
                f'decision_id {self.decision_id} '\
                f'reward {self.reward}' 


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
        return df.astype(DF_SCHEMA)
        

    @staticmethod
    def load_groups(s3_key) -> List[FirehoseRecordGroup]:
        assert(s3_key)
        """
        Load records from a gzipped jsonlines file
        """
        
        records_by_model = {}
        invalid_records = []
        exception_counts = {}
       
        print(f'loading s3://{FIREHOSE_BUCKET}/{s3_key}')
    
        # download and parse the firehose file
        s3obj = s3client.get_object(Bucket=FIREHOSE_BUCKET, Key=s3_key)['Body']

        with gzip.GzipFile(fileobj=s3obj) as gzf:
            for line in gzf.readlines():
    
                try:
                    record = FirehoseRecord(orjson.loads(line))
                    model = record.model
                    
                    if model not in records_by_model:
                        records_by_model[model] = []
                    
                    records_by_model[model].append(record)

                except Exception as e:
                    e_str = repr(e)
                    exception_counts[e_str] = exception_counts.get(e_str, 0) + 1
                    invalid_records.append(line)
                    continue
    
        print(f'valid records: {json_dumps({k: len(v) for k, v in records_by_model.items()})}')
        if len(invalid_records):
            print(f'invalid records: {len(invalid_records)}')
            print(f'parse exceptions: {json_dumps(exception_counts)}')

        results = []
        for model, records in records_by_model.items():
            results.append(FirehoseRecordGroup(model, records))
        
        return results
