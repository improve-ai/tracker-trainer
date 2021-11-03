import pandas as pd
from ksuid import Ksuid
from uuid import uuid4

from config import TRAIN_BUCKET, s3client
from firehose_records import _is_valid_model_name

class RewardedDecisionGroup:


    def __init__(self, model_name, df, s3_key=None):
        assert(_is_valid_model_name(model_name))
        assert(df)

        self.model_name = model_name
        self.df = df
        self.s3_key = s3_key
        self.sort()


    def process(self):

        if self.s3_key:
            # load the existing .parq from s3
            self.load()

        assert(self.sorted)
    
        # merge the rewarded decisions together to accumulate the rewards
        self.merge()
        
        self.save()

        if self.s3_key:
            # delete the previous .parq from s3
            # do this last in case there is a problem during processing that needs to be retried
            self.clean_up()

        # RAM cleanup
        self.clean_memory()


    def load(self):
        # TODO split load into s3 request and parse.  If fail on s3 then throw exception
        # and fail job.  If parse error, move bad records to /unrecoverable 
        s3_df = pd.read_parquet(f's3://{TRAIN_BUCKET}/{self.s3_key}')
        self.df = pd.concat([self.df, s3_df], ignore_index=True)
        self.sorted = False
        self.sort()
        

    def save(self):
        # write the conslidated parquet file to a unique key
        self.df.write_parquet(f's3://{TRAIN_BUCKET}/{s3_key(self.model_name, self.first_decision_id(), self.last_decision_id())}')

        
    def clean_up(self):
        # delete the old s3_key
        s3client.delete_object(Bucket=TRAIN_BUCKET, Key=self.s3_key)
        

    def sort(self):
        # TODO sort by decision_id

        self.sorted = True
        
        
    def merge(self):
        assert self.sorted
        
        # TODO merge rewarded decisions

            
    def clean_memory(self):
        # cleanup
        self.df = None
        del self.df
        

    def first_decision_id(self):
        assert(self.sorted)
        #TODO
        pass
    
    
    def last_decision_id(self):
        assert(self.sorted)
        #TODO
        pass
    
    @staticmethod
    def groups_from_firehose_record_group(firehose_record_group):
        pass

def decision_id_range_from_groups(groups):
    # return first, last
    pass


def s3_key_prefix(model_name, max_decision_id):
    ksuid = Ksuid.from_base62(max_decision_id)
    yyyy, mm, dd = ksuid.datetime.strftime("%Y-%m-%d").split('-')
    # Truncate the ksuid so that we're not exposing entire decision ids in the file names.
    # While having entire ids in the file names is likey no risk, it is also unneccessary.
    return f'/rewarded_decisions/{model_name}/parquet/{yyyy}/{mm}/{dd}/{max_decision_id[:9]}'
    
    
def s3_key(model_name, min_decision_id, max_decision_id):
    # The final KSUID is simply to give the file a random name.  We could have used UUIDv4 here
    # but we're already using KSUID, it's slightly shorter, and slightly easier to parse due
    # to no dashes.  For now, the final KSUID should be considered an opaque string of random characters
    return f'{s3_key_prefix(model_name, max_decision_id)}-{min_decision_id[:9]}-{Ksuid()}.parquet'
