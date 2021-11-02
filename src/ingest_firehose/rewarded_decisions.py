import pandas as pd
from ksuid import ksuid

from config import TRAIN_BUCKET, s3client

class RewardedDecisions:

    def __init__(self, df, s3_key=None):
        assert(df)

        self.df = df
        self.s3_key = s3_key
        self.sort()


    def process(self):

        # load the existing parquet from s3
        if self.s3_key:
            self.load()

        assert(self.sorted)
    
        # merge the rewarded decisions together to accumulate the rewards
        self.merge()
        
        self.save()

        # delete the previous .parq from s3
        # do this last in case there is a problem during processing that needs to be retried
        if self.s3_key:
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
        self.df.write_parquet(f's3://{TRAIN_BUCKET}/{s3_key_for_decisions(df)}')

        
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

def decision_id_range_from_groups(groups):
    # return first, last
    pass


def s3_key_prefix(model, last_decision_id):
    return f'/rewarded_decisions/{model}/parq/{yyyy}/{mm}/{dd}/{yyyy}{mm}{dd}-{last_decision_id[:9]}'
    
def s3_key(model, first_decision_id, last_decision_id):
    return f'{s3_key_prefix(model,last_decision_id)}-{first_decision_id[:9]}.parq'
