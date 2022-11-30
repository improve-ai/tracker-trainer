# Built-in imports
from concurrent.futures import ThreadPoolExecutor


# External imports
from ksuid import Ksuid
import numpy as np
import orjson
import pandas as pd
from uuid import uuid4


# Local imports
from config import s3client, TRAIN_BUCKET, PARQUET_FILE_MAX_DECISION_RECORDS, S3_CONNECTION_COUNT
from firehose_record import DECISION_ID_KEY, REWARD_KEY, DF_SCHEMA, DF_COLUMNS, REWARDS_COLUMN_INDEX, \
    REWARD_COLUMN_INDEX, COUNT_KEY, EMPTY_REWARDS_JSON_ENCODED, NO_REWARDS_REWARD_VALUE
from firehose_record import is_valid_message_id
from utils import is_valid_model_name, is_valid_rewarded_decisions_s3_key, list_s3_keys

ISO_8601_BASIC_FORMAT = '%Y%m%dT%H%M%SZ'

class RewardedDecisionPartition:


    def __init__(self, model_name, df=None, s3_keys=None):
        assert is_valid_model_name(model_name)

        self.model_name = model_name
        self.df = df
        
        if s3_keys:
            assert len(s3_keys) <= 1000 # DeleteObjects takes a maximum of 1000 keys
            
        self.s3_keys = s3_keys
        

        self.sorted = False


    def process(self, use_old_merge: bool = False):

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


    def _merge_many_records_group(
            self, records_array, array_not_nans_mask, group_slice_start, group_slice_end, into):
        """
        Merges a single decision ID group into a single record (into parameter)

        Parameters
        ----------
        records_array: np.ndarray
            a 2D array of fully and partially rewarded decision records
        array_not_nans_mask:
            a 2D boolean array indicating where np.nans are located in the records_array  
        group_slice_start: int
            an index of records_array at which merged records group starts 
        group_slice_end: int
            index value indicating records_array subset end (true group end is at group_end_index - 1)
        into: np.ndarray
            a numpy array into which merging results will be written

        Returns
        -------
        None
            None

        """
        assert group_slice_start >= 0 and group_slice_end > 0 and (group_slice_end - group_slice_start) > 1
        # extract the slice from all records array
        group_slice = records_array[group_slice_start:group_slice_end, :]
        # extract the not np.nans mask slice from all records mask
        group_not_nans_mask = array_not_nans_mask[group_slice_start:group_slice_end, :]

        # for all columns other than "rewards" and "reward" select first not np.nan element
        # np.argmax() allows to select index of first encountered True value in each column of group_not_nans_mask
        # and provides per-column subset indices
        # numpy syntax allows to selecting per-column row cells with the following call:
        # <array>[<per column row indices>, <column indices>]
        into[:REWARDS_COLUMN_INDEX] = group_slice[
            np.argmax((group_not_nans_mask[:, :REWARDS_COLUMN_INDEX]), axis=0), np.arange(0, group_slice[0, :REWARDS_COLUMN_INDEX].shape[0])]

        # find a boolean mask for all not empty (!= "{}") and not np.nan rewards JSONs
        not_empty_rewards = \
            group_not_nans_mask[:, REWARDS_COLUMN_INDEX] * (group_slice[:, REWARDS_COLUMN_INDEX] != EMPTY_REWARDS_JSON_ENCODED)

        # If all cells in "rewards" column are empty it means that resulting reward is 0.0 and rewards equal to "{}"
        if (~not_empty_rewards).all():
            into[REWARDS_COLUMN_INDEX:] = [EMPTY_REWARDS_JSON_ENCODED, NO_REWARDS_REWARD_VALUE]
        else:
            # This is a trick which significantly speeds up an entire merge process
            # "rewards" column stores flat dicts with a simple structure:
            # {<reward message ID>: <reward value>, ...}
            # This means that all not empty entries of "rewards column can be concatenated
            # without loading them by a simple procedure:
            # - strip leading "{" and trailing "}" of each record's "rewards", e.g. (please note that
            # at this point np.nan can be present in "reward" column )
            # -- | ..... |      "rewards"     |  "reward"  |
            # ---|-------|--------------------|------------|
            # 0: | ..... | '{"a": 1, "b": 2}' |   np.nan   |
            # 1: | ..... | '{"c": 1, "d": 2}' |   np.nan   |
            # 2: | ..... |    '{"e": 1}'      |   np.nan   |
            #
            # 'stripped' "rewards" column / array -> ['"a": 1, "b": 2', '"c": 1, "d": 2', '"e": 1']
            # - string concatenation with a ',' and wrapping result with "{" + <string concatenation result> + "}"
            #   will provide a JSON string containing all rewards dicts without even loading it with orjson.loads:
            #   "{" + ','.join(['"a": 1, "b": 2', '"c": 1, "d": 2', '"e": 1']) "}" =
            #   '{"a": 1, "b": 2', "c": 1, "d": 2, "e": 1}'
            # - the last setp is to call orjson.loads() on the concatenated rewards. If the JSON string is broken
            #   orjson.loads() will raise as it would for a single malformed "rewards" element
            # this logic is encapsulated within method concat_rewards_dicts_without_loads()
            loaded_rewards = orjson.loads(
                concat_rewards_dicts_without_loads(group_slice[:, REWARDS_COLUMN_INDEX][not_empty_rewards]))
            # Once all rewards are merged in to a single dict they can be dumped in the "rewards" column of into
            # It is possible to skip orjson.dumps() call (and make a code a bit faster) by working
            # on plain strings all the way for "rewards" column but it is safer to use orjson.dumps()
            into[REWARDS_COLUMN_INDEX] = \
                orjson.dumps(loaded_rewards, option=orjson.OPT_SORT_KEYS).decode('utf-8')
            # reward is a sum of all values() from loaded_rewards
            # if any of the values is malformed (e.g. a nested dict instead of <message id>: <reward> key - value pair
            # this is where error will be raised
            into[REWARD_COLUMN_INDEX] = sum(loaded_rewards.values())


    def _get_group_slicing_indices(self, records_array):
        """
        Utility function for most recent merge() implementation. Assuming self.df
        is sorted by decision ID method attempts to extract groups' start and end
        slicing indices from records_array index

        Parameters
        ----------
        records_array: np.ndarray
            2D array representing all merged records

        Returns
        -------
        tuple
            a tuple of 2 1D arrays: (<groups starts indices>, <groups ends indices>)

        """
        assert self.sorted
        # create an integer index of df_array, fastest way seems to be np.arange()
        # remark: tried np.roll() approach - it provides shorter code but is observabluy slower
        df_array_indices = np.arange(0, records_array.shape[0])
        # create a np array to store operations needed to determine groups starts and ends
        groups_boundaries = np.empty((records_array.shape[0], 3), dtype=object)
        # cache original decision IDs in the first column
        groups_boundaries[:, 0] = records_array[:, 0]
        # 'move down' decision IDs (this creates a 'previous' decision ID column) to be able to compare it with first column
        groups_boundaries[1:, 1] = records_array[:-1, 0]
        # 'move up' decision IDs (this creates a 'next' decision ID column) to be able to compare it with first column
        groups_boundaries[:-1, 2] = records_array[1:, 0]

        # This comparison: groups_boundaries[:, 0] != groups_boundaries[:, 1]
        # creates a boolean mask which shows at which index a group with identical decision ID starts
        # This comparison: groups_boundaries[:, 0] != groups_boundaries[:, 2]
        # creates a boolean mask which shows at which index a group with identical decision ID ends
        # df_array_indices[groups_boundaries[:, 0] != groups_boundaries[:, 1]] selects only those indices for which
        # the logical statement is True -> as a result indices of groups starts are selected
        # Same approach using groups ends mask allows to select indices at which groups end.
        # Remark: groups end indicate exact index at which each group ends.
        # python's array sntax <array>[<start>:<end>] will actually return an array until <end> - 1 index
        # which mean that for such array subsetting the last element of each group would be excluded from the subset.
        # In order to avoid this the 'groups ends' must be incremented by one
        return df_array_indices[groups_boundaries[:, 0] != groups_boundaries[:, 1]], \
            df_array_indices[groups_boundaries[:, 0] != groups_boundaries[:, 2]] + 1

    def _merge_one_record_groups(
            self, records, records_not_nans_mask, records_one_record_groups_starts,
            merged_records_one_record_groups_indices, merged_records):
        """
        Merges all group with single record at once.

        Parameters
        ----------
        records: np.ndarray
            a 2D array of fully and partially rewarded decision records
        records_not_nans_mask:
            a 2D boolean array indicating where np.nans are located in the records_array
        records_one_record_groups_starts: np.ndarray
            this is an array with single record groups indices for records_array
        merged_records_one_record_groups_indices: np.ndarray
            this is an array with single record groups indices for merged_records
        merged_records: np.ndarray
            2D array storing merge results

        Returns
        -------
        None
            None

        """

        # select all single record groups from records_array
        single_record_groups_df_np = records[records_one_record_groups_starts, :]

        # TODO this is valid if reward is a last column (column index = -1)
        #  and rewards is penultimate column (column index = -2)
        # copy all columns but "rewards" and "reward" into merged_records
        # (this is equivalent to selecting first and if possible not np.nan element)
        merged_records[merged_records_one_record_groups_indices, :REWARD_COLUMN_INDEX] = single_record_groups_df_np[:, :REWARD_COLUMN_INDEX]

        # First a mask indicating which elements are not np.nan and != "{}" is created
        rewards_to_parse_filter = \
            records_not_nans_mask[records_one_record_groups_starts, REWARDS_COLUMN_INDEX] * (single_record_groups_df_np[:, REWARDS_COLUMN_INDEX] != '{}')

        # Selecting indices of merged_records which have np.nan or "{}" in "Rewards" columns
        no_rewards_to_parse_indices = merged_records_one_record_groups_indices[~rewards_to_parse_filter]

        # Then all records for which "reward" is either np.nan or "{}" have "{}" assigned to "rewards" column
        # and 0.0 to "reward" column
        merged_records[no_rewards_to_parse_indices, REWARDS_COLUMN_INDEX] = EMPTY_REWARDS_JSON_ENCODED
        merged_records[no_rewards_to_parse_indices, REWARD_COLUMN_INDEX] = NO_REWARDS_REWARD_VALUE

        # Selecting indices of merged_records which need to be merged (have not nulish entries in "rewards" column)
        rewards_to_parse_indices = merged_records_one_record_groups_indices[rewards_to_parse_filter]
        # For all records which have not nullish "rewards" for each record:
        # - orjson.loads() value of "rewards" column for a given record
        # - sum values ot loaded JSON string
        merged_records[rewards_to_parse_indices, REWARD_COLUMN_INDEX] = \
            [sum(orjson.loads(rewards).values()) for rewards in merged_records[rewards_to_parse_indices, REWARDS_COLUMN_INDEX]]

    def merge(self):
        # make sure that df is sorted -> this is crucial for current merge() implementation
        assert self.sorted
        # since records is of object type np.isnan() won't work
        # an alternative approach is to use elementwise multiplication by 0 which
        # will make all not nan values become empty strings or 0s while nans will
        # remain 'untouched'
        # extract numpy array from pandas DF
        records = self.df.values

        # Saving file to parquet converts all NaN values in object typed columns to None
        # This means that if a s3 key from train bucket is being merged along with gzipped jsonlines
        # the self.df contains both NaNs and Nones in object typed columns. This raises problems
        # with fast missing values mask creation:
        # - np.isnan() does not work with object dtypes
        # - None is not of a float type and will cause records * np.full(records.shape, 0) to fail
        # In order to get the element-wise multiplication to work all Nones must be replaced with np.nans
        if self.s3_keys is not None:
            records[records == None] = np.nan

        # In order to create a boolean mask indicating which cell contains not np.nan value
        # 2 steps are needed:
        # - records multiplication by an array full of 0s -> strings will become '', numbers
        # will become 0 / 0.0 and np.nans will remain np.nans
        nans_filtering_container = records * np.full(records.shape, 0)
        # - simple comparison of multiplication result with '' and 0 / 0.0 allows to create a
        # boolean mask indicating where np.nans are located
        records_not_nans_mask = (nans_filtering_container == '') + (nans_filtering_container == 0)

        # Since the df values are sorted by decision ids the df is already grouped.
        # What remains to be determined is finding groups start and end indices
        # This can be done by simple approach:
        # - cache decision id column
        # - roll decision ids 1 cell down and cache to another column -> this is the 'previous' decision ID for each row
        # - roll decision ids 1 cell up and cache to another column -> this is the 'next' decision ID for each row
        # rows where decision id != 'previous decision id' indicate groups starts
        # rows where decision id != 'next decision id' indicate groups ends
        # this procedure is implemented according to numpy's vectorized paradigm in the get_decision_id_groups_starts_ends()
        groups_slices_starts, groups_slices_ends = self._get_group_slicing_indices(records)

        # Create a placeholder for merging results (number of records should be
        # equal to number of groups / unique decision ids)
        merged_records = np.full((groups_slices_starts.shape[0], records.shape[1]), np.nan, dtype=object)

        # Assuming that the rewards will be sparse it is expected to see most of the
        # groups have only 1 record. Such groups can be processed 'together'
        # First step is the identification of the groups which have more than 1 record to merge
        is_many_records_group = (groups_slices_ends - groups_slices_starts) > 1

        # All indices of merged groups are prepared
        merged_records_index = np.arange(0, merged_records.shape[0])
        # processing groups with single record
        # merged_records_index[~is_many_records_group] -> indices of results for groups with only one record to merge
        self._merge_one_record_groups(
            records, records_not_nans_mask, groups_slices_starts[~is_many_records_group],
            merged_records_index[~is_many_records_group], merged_records)
        # processing groups with multiple records
        # merged_records_index[is_many_records_group] -> indices of results for groups with multiple records to merge
        for i in merged_records_index[is_many_records_group]:
            self._merge_many_records_group(
                records, records_not_nans_mask, groups_slices_starts[i], groups_slices_ends[i], merged_records[i])

        # final df with merged records is created
        self.df = pd.DataFrame(merged_records, columns=DF_COLUMNS)

        # only 2 columns need to be cast to floats (by default the DF infers object column type)
        for column_name in [COUNT_KEY, REWARD_KEY]:
            self.df[column_name] = self.df[column_name].astype('float64')


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

    return "{" + ','.join([rewards[1:-1] for rewards in rewards_jsons]) + "}"
