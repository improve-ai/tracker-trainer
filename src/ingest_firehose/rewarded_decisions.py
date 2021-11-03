import pandas as pd
from ksuid import Ksuid
from uuid import uuid4

from config import TRAIN_BUCKET, s3client
from firehose_records import _is_valid_model_name, DECISION_ID_KEY


class RewardedDecisionGroup:


    def __init__(self, model_name, df, s3_key=None):
        assert _is_valid_model_name(model_name)
        assert df

        self.model_name = model_name
        self.df = df
        self.s3_key = s3_key


    def process(self):

        # load the existing .parquet file (if any) from s3
        self.load()
        
        # remove any invalid rows
        self.filter_valid()

        # sort the combined dataframe and update min/max decision_ids
        self.sort()

        # merge the rewarded decisions together to accumulate the rewards
        self.merge()
        
        # save the consolidated .parquet file to s3
        self.save()

        # delete the old .parquet file (if any) and clean up dataframe RAM
        self.cleanup()


    def load(self):
        if not self.s3_key:
            # nothing to load, just use the incoming firehose records
            return

        # TODO split load into s3 request and parse.  If fail on s3 then throw exception
        # and fail job.  If parse error, move bad records to /unrecoverable 
        s3_df = pd.read_parquet(f's3://{TRAIN_BUCKET}/{self.s3_key}')
        self.df = pd.concat([self.df, s3_df], ignore_index=True)


    def save(self):
        # write the conslidated parquet file to a unique key
        self.df.write_parquet(f's3://{TRAIN_BUCKET}/{s3_key(self.model_name, self.min_decision_id, self.max_decision_id)}')

    
    def filter_valid(self):
        # TODO remove any rows with invalid decision_ids, update stats, copy to /unrecoverable (lower priority)
        pass
    
    
    def sort(self):
        self.df.sort_values(DECISION_ID_KEY)
        
        self._min_decision_id = self.df[DECISION_ID_KEY].min()
        self._max_decision_id = self.df[DECISION_ID_KEY].max()

        self.sorted = True
    
    @property    
    def min_decision_id(self):
        assert self.sorted
        # use instance variable because it will be accessed after dataframe cleanup
        return self._min_decision_id
    
    @property
    def max_decision_id(self):
        assert self.sorted
        # use instance variable because it will be accessed after dataframe cleanup
        return self._max_decision_id
        
        
    def merge(self):
        assert self.sorted
        
        # TODO merge rewarded decisions


    def cleanup(self):
        if self.s3_key:
            # delete the previous .parq from s3
            # do this last in case there is a problem during processing that needs to be retried
            s3client.delete_object(Bucket=TRAIN_BUCKET, Key=self.s3_key)

        # reclaim the dataframe memory
        # do not clean up min/max decision_ids since they will need to be used after processing
        # for determining if any of the .parquet files have overlapping decision_ids
        self.df = None
        del self.df
        
    
    @staticmethod
    def groups_from_firehose_record_group(firehose_record_group):
        # TODO list against S3 to find existing s3_keys that need to be loaded.  Split the record
        # group into groupus by S3 key.
        return [firehose_record_group]


def min_max_decision_ids(decision_groups):
    min_decision_id = min(map(lambda x: x.min_decision_id, decision_groups))
    max_decision_id = max(map(lambda x: x.max_decision_id, decision_groups))
    return min_decision_id, max_decision_id


def repair_overlapping_keys(model_name, decision_groups):
    for decision_group in decision_groups:
        assert decision_group.model_name == model_name
        
    min_decision_id, max_decision_id = min_max_decision_ids(decision_groups)
    
    # TODO keep iterating until all overlapping keys have been repaired.  It may take multiple passes
    # TODO this is low priority for initial release since overlapping keys should be very rare
    
    return


def s3_key_prefix(model_name, max_decision_id):
    ksuid = Ksuid.from_base62(max_decision_id)
    yyyy, mm, dd = ksuid.datetime.strftime("%Y-%m-%d").split('-')
    
    #
    # Truncate the ksuid so that we're not exposing entire decision ids in the file names.
    # While having entire ids in the file names is likey no risk, it is also unneccessary.
    #
    # The max decision_id is encoded first in the path so that a lexicographically sorted
    # search of file names starting at the prefix of the target decision_id will provide
    # the .parquet that should contain that decision_id, if it exists
    #
    return f'/rewarded_decisions/{model_name}/parquet/{yyyy}/{mm}/{dd}/{max_decision_id[:9]}'
    
    
def s3_key(model_name, min_decision_id, max_decision_id):
    #
    # The min decision_id is encoded into the file name so that a lexicographically ordered listing
    # can determine if two parquet files have overlapping decision_id ranges, which they should not.
    # If overlapping ranges are detected they should be repaired by loading the overlapping parquet
    # files, consolidating them, optionally splitting, then saving.  This process should lead to
    # eventually consistency.
    #
    # The final KSUID is simply to give the file a random name.  We could have used UUIDv4 here
    # but we're already using KSUID, it's slightly shorter, and slightly easier to parse due
    # to no dashes.  For now, the final KSUID should be considered an opaque string of random characters
    #
    return f'{s3_key_prefix(model_name, max_decision_id)}-{min_decision_id[:9]}-{Ksuid()}.parquet'
