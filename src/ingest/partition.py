# Built-in imports
from collections import ChainMap
import itertools
from typing import List
import math
from concurrent.futures import ThreadPoolExecutor


# External imports
from ksuid import Ksuid
import numpy as np
import orjson
import pandas as pd
from uuid import uuid4


# Local imports
from config import s3client, TRAIN_BUCKET, PARQUET_FILE_MAX_DECISION_RECORDS, S3_CONNECTION_COUNT
from firehose_record import DECISION_ID_KEY, REWARDS_KEY, REWARD_KEY, DF_SCHEMA, DF_COLUMNS, \
    REWARDS_COLUMN_INDEX, REWARD_COLUMN_INDEX, COUNT_KEY, EMPTY_REWARDS_JSON_ENCODED, NO_REWARDS_REWARD_VALUE
from firehose_record import is_valid_message_id
from utils import is_valid_model_name, is_valid_rewarded_decisions_s3_key, json_dumps, list_s3_keys

ISO_8601_BASIC_FORMAT = '%Y%m%dT%H%M%SZ'
skip_first_and_last_char = np.vectorize(lambda x: x[1:-1])

class RewardedDecisionPartition:


    def __init__(self, model_name, df=None, s3_keys=None):
        assert is_valid_model_name(model_name)

        self.model_name = model_name
        self.df = df
        
        if s3_keys:
            assert len(s3_keys) <= 1000 # DeleteObjects takes a maximum of 1000 keys
            
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
                dfs_concat = pd.concat(dfs, ignore_index=True)

                if self.df is not None and isinstance(self.df, pd.DataFrame):
                    dfs.append(self.df)

                self.df = pd.concat(dfs, ignore_index=True).astype(DF_SCHEMA)
                self.sorted = False

            print(f'loaded {self.df.shape[0]} rewarded decisions for {self.model_name} across {len(self.s3_keys)} partitions')
                

    def save(self):
        assert self.sorted
        
        # disperse overlaps throughout the index
        chunks = maybe_split_on_timestamp_boundaries(self.df)

        print(f'writing {sum(map(lambda x: x.shape[0], chunks))} rewarded decisions for {self.model_name} across {len(chunks)} partitions')
        
        # split the dataframe into multiple chunks if necessary
        for chunk in chunks:
            # generate a unique s3 key for this chunk
            chunk_s3_key = parquet_s3_key(self.model_name, min_decision_id=chunk[DECISION_ID_KEY].iat[0], 
                max_decision_id=chunk[DECISION_ID_KEY].iat[-1], count=chunk.shape[0])
                
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

    def merge_many_records_groups(
            self, df_np, not_nans_mask, group_start_index, group_end_index, into):

        # select all records for currently processed decision ID
        group_np = df_np[group_start_index:group_end_index, :]
        # select a subset of mask indicating not nullish entries in processed df
        group_not_nans_mask = not_nans_mask[group_start_index:group_end_index, :]

        into[:REWARDS_COLUMN_INDEX] = group_np[np.argmax((group_not_nans_mask[:, :-2]), axis=0), np.arange(0, group_np.shape[1] - 2)]

        if ~(group_not_nans_mask[:, REWARDS_COLUMN_INDEX].any()):
            into[REWARDS_COLUMN_INDEX:] = [EMPTY_REWARDS_JSON_ENCODED, NO_REWARDS_REWARD_VALUE]
        else:

            # TODO test this thoroughly
            # TODO test this with an already processed s3 df

            # if only empty dicts are in the 'rewards' column this means that
            # resulting rewards is also an empty dict and resulting reward is 0.0
            not_empty_rewards = \
                group_not_nans_mask[:, REWARDS_COLUMN_INDEX] * \
                (group_np[:, REWARDS_COLUMN_INDEX] != EMPTY_REWARDS_JSON_ENCODED)

            if ~(not_empty_rewards.any()):
                into[REWARDS_COLUMN_INDEX:] = [EMPTY_REWARDS_JSON_ENCODED, NO_REWARDS_REWARD_VALUE]
            else:
                loaded_rewards = orjson.loads(concat_rewards_dicts_without_loads(group_np[:, REWARDS_COLUMN_INDEX][not_empty_rewards]))
                into[REWARDS_COLUMN_INDEX] = \
                    orjson.dumps(loaded_rewards, option=orjson.OPT_SORT_KEYS).decode('utf-8')
                into[REWARD_COLUMN_INDEX] = sum(loaded_rewards.values())

    def get_decision_id_groups_starts_ends(self, df_np):
        indices = np.arange(0, df_np.shape[0])
        # remark: tried np.roll() approach - it is more concise but slower
        split_indices_df = np.empty((df_np.shape[0], 3), dtype=object)
        split_indices_df[:, 0] = df_np[:, 0]
        split_indices_df[1:, 1] = df_np[:-1, 0]
        split_indices_df[:-1, 2] = df_np[1:, 0]

        return indices[split_indices_df[:, 0] != split_indices_df[:, 1]], \
            indices[split_indices_df[:, 0] != split_indices_df[:, 2]] + 1

    def merge_one_record_groups(
            self, df_np, not_nans_mask, one_record_groups_starts,
            is_one_record_group_mask, one_record_groups_indices, merged_records):

        # TODO work on direct indexing ->
        single_record_groups_df_np = df_np[one_record_groups_starts, :]

        # TODO this is valid if reward is a last column (column index = -1)
        #  and rewards is penultimate column (column index = -2)
        merged_records[is_one_record_group_mask, :REWARD_COLUMN_INDEX] = single_record_groups_df_np[:, :REWARD_COLUMN_INDEX]

        # assign '{}' and 0.0 to indices which store '{}' or nan under rewards key
        # single_row_groups_rewards_not_nan_mask = not_nans_mask[single_record_groups_start_indices, REWARDS_COLUMN_INDEX]
        # not_empty_dicts_rewards_nan_mask = df_np[single_record_groups_start_indices, :REWARDS_COLUMN_INDEX] != '{}'

        rewards_to_parse_filter = \
            not_nans_mask[one_record_groups_starts, REWARDS_COLUMN_INDEX] * (single_record_groups_df_np[:, REWARDS_COLUMN_INDEX] != '{}')
        no_rewards_to_parse_indices = one_record_groups_indices[~rewards_to_parse_filter]
        rewards_to_parse_indices = one_record_groups_indices[rewards_to_parse_filter]

        merged_records[no_rewards_to_parse_indices, REWARDS_COLUMN_INDEX] = EMPTY_REWARDS_JSON_ENCODED
        merged_records[no_rewards_to_parse_indices, REWARD_COLUMN_INDEX] = NO_REWARDS_REWARD_VALUE

        merged_records[rewards_to_parse_indices, REWARD_COLUMN_INDEX] = \
            [sum(orjson.loads(rewards).values()) for rewards in merged_records[rewards_to_parse_indices, REWARDS_COLUMN_INDEX]]

    def merge(self):
        # make sure that df is sorted -> this is crucial for current merge() implementation
        assert self.sorted
        # since df_array is of object type np.isnan() won't work
        # an alternative approach is to use elementwise multiplication by 0 which
        # will make all not nan values become empty strings or 0s while nans will
        # remain 'untouched'
        # extract numpy array from pandas DF
        df_array = self.df.values

        # Saving file to parquet converts all NaN values in object typed columns to None
        # This means that if a s3 key from train bucket is being merged along with gzipped jsonlines
        # the self.df contains both NaNs and Nones in object typed columns. This raises problems
        # with fast missing values mask creation:
        # - np.isnan() does not work with object dtypes
        # - None is not of a float type and will cause df_array * np.full(df_array.shape, 0) to fail
        # In order to get the element-wise multiplication to work all Nones must be replaced with np.nans
        if self.s3_keys is not None:
            df_array[df_array == None] = np.nan

        # In order to create a boolean mask indicating which cell contains not np.nan value
        # 2 steps are needed:
        # - df_array multiplication by an array full of 0s -> strings will become '', numbers
        # will become 0 / 0.0 and np.nans will remain np.nans
        nans_filtering_container = df_array * np.full(df_array.shape, 0)
        # - simple comparison of multiplication result with '' and 0 / 0.0 allows to create a
        # boolean mask indicating where np.nans are located
        df_not_nans_mask = (nans_filtering_container == '') + (nans_filtering_container == 0)

        # Since the df values are sorted by decision ids the df is already grouped.
        # What remains to be determined is finding groups start and end indices
        # This can be done by simple approach:
        # - cache decision id column
        # - roll decision ids 1 cell down and cache to another column -> this is the 'previous' decision ID for each row
        # - roll decision ids 1 cell up and cache to another column -> this is the 'next' decision ID for each row
        # rows where decision id != 'previous decision id' indicate groups starts
        # rows where decision id != 'next decision id' indicate groups ends
        # this procedure is implemented according to numpy's vectorized paradigm in the get_decision_id_groups_starts_ends()
        groups_starts, groups_ends = self.get_decision_id_groups_starts_ends(df_array)

        # Create a placeholder for merging results (number of records should be
        # equal to number of groups / unique decision ids)
        merged_records = np.full((groups_starts.shape[0], df_array.shape[1]), np.nan, dtype=object)

        # Assuming that the rewards will be sparse it is expected to see most of the
        # groups have only 1 record. Such groups can be processed 'together'
        # First step is the identification of the groups which have more than 1 record to merge
        is_many_records_group = (groups_ends - groups_starts) > 1

        # All indices of merged groups are prepared
        groups_merged_records_index = np.arange(0, merged_records.shape[0])

        # indices of results for groups with only one record to merge
        one_record_groups_indices = groups_merged_records_index[~is_many_records_group]
        # indices of results for groups with multiple records to merge
        many_records_groups_indices = groups_merged_records_index[is_many_records_group]

        # processing groups with single record
        self.merge_one_record_groups(
            df_array, df_not_nans_mask, groups_starts[~is_many_records_group], ~is_many_records_group,
            one_record_groups_indices, merged_records)

        # processing groups with multiple records
        for i in many_records_groups_indices:
            self.merge_many_records_groups(
                df_array, df_not_nans_mask, groups_starts[i], groups_ends[i], merged_records[i])

        # final df with merged records is created
        self.df = pd.DataFrame(merged_records, columns=DF_COLUMNS)

        # only 2 columns need to be cast to floats (by default the DF infers object column type)
        for cn in [COUNT_KEY, REWARD_KEY]:
            self.df[cn] = self.df[cn].astype('float64')

    def old_merge(self):
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


    def cleanup(self):
        if self.s3_keys:
            # delete the previous .parquet files from s3
            # do this last in case there is a problem during processing that needs to be retried
        
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

    s3_df = pd.read_parquet(f's3://{TRAIN_BUCKET}/{s3_key}')

    # TODO: add more validations
    valid_idxs = s3_df.decision_id.apply(is_valid_message_id)
    if not valid_idxs.all():
        unrecoverable_key = f'unrecoverable/{s3_key}'

        s3_df.to_parquet(f's3://{TRAIN_BUCKET}/{unrecoverable_key}', compression='ZSTD')
        
        s3client.delete_object(Bucket=TRAIN_BUCKET, Key=s3_key)

        raise IOError(f"Invalid records found in '{s3_key}'. Moved to s3://{TRAIN_BUCKET}/{unrecoverable_key}'")

    return s3_df
    
    
def maybe_split_on_timestamp_boundaries(df, max_row_count=PARQUET_FILE_MAX_DECISION_RECORDS):
    ''' The purposes of this is to disperse overlaps throughout the timeline. The common case for
    row_count > max is when merging an overlap.  It is also possible for the split to be triggered
    if ingesting a very large firehose file or in the exceptional case where > max decisions occur on 
    the same second. 
    
    Splitting on timestamp boundaries serves two purposes. First, by having hard boundaries it reduces
    chains of overlaps propegating back through the timeline. Note that each more finegrained timestamp
    prefix includes its parents, so the boundaries are enforced at all resolutions. Second, the
    timestamp boundaries quickly disperse rewards for old decisions further back in the timeline so that
    they may be merged in just a few grooming passes.
    
    Rather than fixing the timestamp boundaries via a configuration, this allows the timestamp boundaries
    to dynamically scale with the rate of decision tracking.
    
    I haven't proved this rigorously but in practice it seems like this resolves overlaps in something 
    like O(log(N)) iterations of the grooming process.
    '''
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
    maxts, mints, count_str = s3_key.split('/')[-1].split('-')[:3]
    return mints, maxts, int(count_str)
    
    
def row_count(s3_key):
    _, _, row_count = min_max_timestamp_row_count(s3_key)
    return row_count
    
    
def min_timestamp(s3_key):
    mints, _, _ = min_max_timestamp_row_count(s3_key)
    return mints


def max_timestamp(s3_key):
    _, maxts, _ = min_max_timestamp_row_count(s3_key)
    return maxts
    
    
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
    return filter(is_valid_rewarded_decisions_s3_key,
                  list_s3_keys(bucket_name=TRAIN_BUCKET, prefix=f'rewarded_decisions/{model_name}/parquet/'))


def concat_rewards_then_json_loads(rewards_jsons):
    """
    Concatenates JSON strings before calling orjson.loads() to save runtime:
    From a collection of JSON dict strings, e.g.:
    ['{"a": 1}', '{"b": 2}', '{"c": 3}']
    creates a single string:
    '{"a": 1, "b": 2, "c": 3}'
    and then calls orjson.loads() on it.
    This way orjson.loads() is only called once

    Parameters
    ----------
    rewards_jsons: list or np.ndarray
        list of JSON strings with rewards dicts

    Returns
    -------
    dict
        a JSON loaded concat of input rewards JSONS

    """

    return orjson.loads("[" + ','.join(rewards_jsons) + "]")


def concat_rewards_dicts_without_loads(rewards_jsons):
    """
    Reward dicts can be concatenated without even calling orjson loads. Each of the
    reward dicts is flat and has a <string> : <float> structure. This means that
    the concatenation on JSON strings can go as follows:
    1. for each JSON string skip '{' and '}'
    2. concatenate strings with ','
    3. add '{' as a first character and '}' as a last character to the resulting string


    Parameters
    ----------
    rewards_jsons

    Returns
    -------

    """

    # return '{' + ','.join(np.hstack(np.char.split(np.char.rstrip(np.char.lstrip(rewards_jsons.astype(str), chars='{'), chars='}'), ','))) + '}'
    # return "{" + ','.join([rewards[1:-1] for rewards in np.unique(rewards_jsons)]) + "}"
    return "{" + ','.join([rewards[1:-1] for rewards in rewards_jsons]) + "}"
