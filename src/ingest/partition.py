# Built-in imports
from collections import ChainMap
import itertools
from typing import List
import math
from concurrent.futures import ThreadPoolExecutor


# External imports
from ksuid import Ksuid
import orjson
import pandas as pd
from uuid import uuid4


# Local imports
from config import s3client, TRAIN_BUCKET, PARQUET_FILE_MAX_DECISION_RECORDS, S3_CONNECTION_COUNT, stats
from firehose_record import DECISION_ID_KEY, REWARDS_KEY, REWARD_KEY, DF_SCHEMA
from firehose_record import is_valid_message_id
from utils import is_valid_model_name, json_dumps, list_s3_keys


ISO_8601_BASIC_FORMAT = '%Y%m%dT%H%M%SZ'


class RewardedDecisionPartition:


    def __init__(self, model_name, df=None, s3_keys=None):
        assert is_valid_model_name(model_name)

        self.model_name = model_name
        self.df = df
        self.s3_keys = s3_keys

        self.sorted = False


    def process(self):

        # load the existing .parquet files (if any) from s3
        self.load()
        
        # remove any invalid rows
        self.filter_valid()

        # sort the combined dataframe and update min/max decision_ids
        self.sort()

        # merge the rewarded decisions together to accumulate the rewards
        self.merge()
        
        # save the consolidated partition to s3 as one or more .parquet files
        self.save()

        # delete the old .parquet files (if any) and clean up dataframe RAM
        self.cleanup()


    def load(self):
        if self.s3_keys:
            with ThreadPoolExecutor(max_workers=S3_CONNECTION_COUNT) as executor:
                dfs = list(executor.map(read_parquet, self.s3_keys))
                
                if self.df:
                    dfs.append(self.df)
                    
                self.df = pd.concat(dfs, ignore_index=True)
                self.sorted = False

#        stats.increment_rewarded_decision_count(self.model_name, self.df.shape[0], s3_df.shape[0])


    def save(self):
        assert self.sorted

        # split the dataframe into multiple chunks if necessary
        for chunk in maybe_split_on_timestamp_boundaries(self.df):
            # generate a unique s3 key for this chunk
            chunk_s3_key = parquet_s3_key(self.model_name, min_decision_id=chunk[DECISION_ID_KEY].iat[0], 
                max_decision_id=chunk[DECISION_ID_KEY].iat[-1], count=chunk.shape[0])
                
            stats.increment_s3_requests_count('put')
            chunk.to_parquet(f's3://{TRAIN_BUCKET}/{chunk_s3_key}', compression='ZSTD', index=False)

    
    def filter_valid(self):
        # TODO we might not do this since unrecoverable copying now happens in read_parquet
        pass
    
    
    def sort(self):
        self.df.sort_values(DECISION_ID_KEY, inplace=True, ignore_index=True)
        
        self.sorted = True
        
    
    @property    
    def min_decision_id(self):
        assert self.sorted
        return self.df[DECISION_ID_KEY].iat[0]
        
    
    @property
    def max_decision_id(self):
        assert self.sorted
        return self.df[DECISION_ID_KEY].iat[-1]


    def merge(self):
        """
        Merge full or partial "rewarded decision records".
        This process is idempotent. It may be safely repeated on 
        duplicate records and performed in any order.
        If fields collide, one will win, but which one is unspecified.  

        """
        
        assert self.sorted

        def merge_rewards(rewards_series):
            """Shallow merge of a list of dicts"""
            rewards_dicts = rewards_series.dropna().apply(lambda x: orjson.loads(x))
            return json_dumps(dict(ChainMap(*rewards_dicts)))

        def sum_rewards(rewards_series):
            """ Sum all the merged rewards values """
            merged_rewards = orjson.loads(merge_rewards(rewards_series))
            return float(sum(merged_rewards.values()))

        def get_first_cell(col_series):
            """Return the first cell of a column """
            
            if col_series.isnull().all():
                first_element = col_series.iloc[0]
            else:
                first_element = col_series.dropna().iloc[0]

                if col_series.name == "count":
                    return first_element.astype("int64")
            return first_element


        non_reward_keys = [key for key in self.df.columns if key not in [REWARD_KEY, REWARDS_KEY]]

        # Create dict of aggregations with cols in the same order as the expected result
        aggregations = { key : pd.NamedAgg(column=key, aggfunc=get_first_cell) for key in non_reward_keys }

        if REWARDS_KEY in self.df.columns:
            aggregations[REWARDS_KEY] = pd.NamedAgg(column="rewards", aggfunc=merge_rewards)
            aggregations[REWARD_KEY]  = pd.NamedAgg(column="rewards", aggfunc=sum_rewards)
        
        """
        Now perform the aggregations. This is how it works:
        
        1) "groupby" creates subsets of the original DF where each subset 
        has rows with the same decision_id.
        
        2) "agg" uses the aggregations dict to create a new row for each 
        subset. The columns will be new and named after each key in the 
        aggregations dict. The cell values of each column will be based on 
        the NamedAgg named tuple, specified in the aggregations dict.
        
        3) These NamedAgg named tuples specify which column of the subset 
        will be passed to the specified aggregation functions.
        
        4) The aggregation functions process the values of the passed column 
        and return a single value, which will be the contents of the cell 
        in a new column for that subset.
        
        Example:
        
        >>> df = pd.DataFrame({
        ...     "A": [1, 1, 2, 2],
        ...     "B": [1, 2, 3, 4],
        ...     "C": [0.362838, 0.227877, 1.267767, -0.562860],
        ... })

        >>> df
           A  B         C
        0  1  1  0.362838
        1  1  2  0.227877
        2  2  3  1.267767
        3  2  4 -0.562860

        >>> df.groupby("A").agg(
        ...     b_min=pd.NamedAgg(column="B", aggfunc="min"),
        ...     c_sum=pd.NamedAgg(column="C", aggfunc="sum")
        ... )
            b_min     c_sum
        A
        1      1  0.590715
        2      3  0.704907
        """

        self.df = self.df.groupby("decision_id").agg(**aggregations).reset_index(drop=True).astype(DF_SCHEMA)
        stats.increment_records_after_merge_count(self.df.shape[0])


    def cleanup(self):
        if self.s3_keys:
            # delete the previous .parqet from s3
            # do this last in case there is a problem during processing that needs to be retried
        
            stats.increment_s3_requests_count('delete')
            response = s3client.delete_objects(
                Bucket=TRAIN_BUCKET,
                Delete={
                    'Objects': [{'Key': s3_key} for s3_key in self.s3_keys],
                },
            )

        # reclaim the dataframe memory
        self.df = None
        del self.df


def read_parquet(s3_key):

    stats.increment_s3_requests_count('get')
    s3_df = pd.read_parquet(f's3://{TRAIN_BUCKET}/{s3_key}')

    # TODO: add more validations
    valid_idxs = s3_df.decision_id.apply(is_valid_message_id)
    if not valid_idxs.all():
        unrecoverable_key = f'unrecoverable/{s3_key}'
        stats.remember_bad_s3_parquet_file(unrecoverable_key)

        stats.increment_s3_requests_count('put')
        s3_df.to_parquet(f's3://{TRAIN_BUCKET}/{unrecoverable_key}', compression='ZSTD')
        
        stats.increment_s3_requests_count('delete')
        s3client.delete_object(Bucket=TRAIN_BUCKET, Key=s3_key)

        raise IOError(f"Invalid records found in '{s3_key}'. Moved to s3://{TRAIN_BUCKET}/{unrecoverable_key}'")

    return s3_df
    
    
def maybe_split_on_timestamp_boundaries(df, max_row_count=PARQUET_FILE_MAX_DECISION_RECORDS):
    
    dfs = [df]
    
    # iterate through different timestamp prefix lengths
    # does not split below 1 second resolution
    for i in range(len('YYYYmm'),len('YYYYmmddTHHMMSS')+1):
        # if all the dataframes are small enough don't split further
        if all(map(lambda x: x.shape[0] <= max_row_count, dfs)):
            break
        
        # group by timestamp prefixes of length i
        dfs = [x.reset_index() for _, x in df.set_index(DECISION_ID_KEY).groupby(lambda x: decision_id_to_timestamp(x)[:i])]
    
    return dfs
    

def min_max_timestamp_row_count(s3_key):
    maxts, mints, count = s3_key.split('/')[-1].split('-')[:3]
    return mints, maxts, count
    
    
def row_count(s3_key):
    _, _, row_count = min_max_timestamp_row_count(s3_key)
    return row_count
    
    
def decision_id_to_timestamp(decision_id):
    return Ksuid.from_base62(decision_id).datetime.strftime(ISO_8601_BASIC_FORMAT)
    

def parquet_s3_key_prefix(model_name, max_decision_id):
    max_timestamp = decision_id_to_timestamp(max_decision_id)
    
    yyyy = max_timestamp[0:4]
    mm = max_timestamp[4:6]
    dd = max_timestamp[6:8]
    
    # The max timestamp is encoded first in the path so that a lexicographically sorted
    # search of file names starting at the prefix of the target decision_id will provide
    # the .parquet that should contain that decision_id, if it exists
    return f'rewarded_decisions/{model_name}/parquet/{yyyy}/{mm}/{dd}/{max_timestamp}'
    
    
def parquet_s3_key(model_name, min_decision_id, max_decision_id, count):
    min_timestamp = decision_id_to_timestamp(min_decision_id)
    
    #
    # The min timestamp is encoded into the file name so that a lexicographically ordered listing
    # can determine if two parquet files have overlapping decision_id ranges, which they should not.
    # If overlapping ranges are detected they should be repaired by loading the overlapping parquet
    # files, consolidating them, optionally splitting, then saving.  This process should lead to
    # eventually consistency.
    #
    # The final UUID4 is simply to give the file a random name. For now, the characters following
    # the third dash should be considered an opaque string of random characters
    #
    return f'{parquet_s3_key_prefix(model_name, max_decision_id)}-{min_timestamp}-{count}-{uuid4()}.parquet'


def list_partition_s3_keys(model_name):
    keys = list_s3_keys(bucket_name=bucket_name, prefix=f'rewarded_decisions/{model_name}/parquet/')
    return keys if not valid_keys_only else [k for k in keys if is_valid_rewarded_decisions_s3_key(k)]
