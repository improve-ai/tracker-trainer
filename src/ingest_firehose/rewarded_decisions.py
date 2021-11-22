# Built-in imports
from collections import ChainMap
import re
import os
from collections import defaultdict

# External imports
import pandas as pd
from ksuid import Ksuid
import orjson

# Local imports
from config import TRAIN_BUCKET, s3client
from firehose_record import FirehoseRecordGroup
from firehose_record import DECISION_ID_KEY, REWARDS_KEY, REWARD_KEY, DF_SCHEMA
from utils import is_valid_model_name, json_dumps
from utils import list_s3_keys_containing

ISO_8601_BASIC_FORMAT = '%Y%m%dT%H%M%SZ'

class RewardedDecisionPartition:


    def __init__(self, model_name, df, s3_key=None):
        assert is_valid_model_name(model_name)
        # TODO assert df and df.shape[0] > 0 # must have some rows

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
        self.df.to_parquet(f's3://{TRAIN_BUCKET}/{s3_key(self.model_name, self.min_decision_id, self.max_decision_id)}', compression='ZSTD')

    
    def filter_valid(self):
        # TODO remove any rows with invalid decision_ids, update stats, copy to /unrecoverable (lower priority)
        pass
    
    
    def sort(self):
        self.df.sort_values(DECISION_ID_KEY, inplace=True, ignore_index=True)
        
        self._min_decision_id = self.df[DECISION_ID_KEY].iat[0]
        self._max_decision_id = self.df[DECISION_ID_KEY].iat[-1]

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
        """
        Merge full or partial "rewarded decision records".
        This process is idempotent. It may be safely repeated on 
        duplicate records and performed in any order.
        If fields collide, one will win, but which one is unspecified.  
        
        Parameters
        ----------
        rewarded_decision_records : pandas.DataFrame
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
                first_element = col_series[0]
            else:
                first_element = col_series.dropna()[0]

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
        if self.s3_key:
            # delete the previous .parqet from s3
            # do this last in case there is a problem during processing that needs to be retried
            s3client.delete_object(Bucket=TRAIN_BUCKET, Key=self.s3_key)

        # reclaim the dataframe memory
        # do not clean up min/max decision_ids since they will need to be used after processing
        # for determining if any of the .parquet files have overlapping decision_ids
        self.df = None
        del self.df
        
    
    @staticmethod
    def partitions_from_firehose_record_group(firehose_record_group):
        """
        High Level Algorithm:
        
        1)  For each decision_id that is being ingested, we need to check if that decision_id is already covered by an existing partition
        within S3.
        
        2)  Each partition file S3 key starts with a prefix of the max KSUID in that partition and contains more characters after that.
        
        3)  S3's list_objects_v2 request returns results in lexicographical order, using the StartAfter option using a prefix key generated
            from the target decision_id, the first result will be the target partition.  If there are no results
            then the max decision_id in all partitions is less than the current one, so there will be no existing partition returned
            and a new partition file will be created.

        4)  Sending a seperate list_objects_v2 request for each decision_id would be extremely slow and expensive.  Due to S3's lexicographically
            sorted list_objects_v2 results, rather than send one list request per decision_id to S3, we just send a few list requests for 
            the range of decision_id prefixes that we’re interested in.  We then use those listing results to partition the decisions by the 
            s3_key that may contain the same decision_ids.
        """

        def decision_id_belongs_to_range(decision_id: str, min_id: str, max_id: str):
            return (decision_id >= min_id.ljust(27, "0")) and (decision_id <= max_id.ljust(27, "0"))
        
        model_name = firehose_record_group.model_name
        
        rdrs = firehose_record_group.to_rewarded_decision_dicts()
        
        # Get the range of decision ids
        rdrs.sort(key=lambda rdr: rdr['decision_id'])
        min_decision_id = rdrs[0]['decision_id']
        max_decision_id = rdrs[-1]['decision_id']
        start_after_key = s3_key_prefix(model_name, min_decision_id)
        end_key         = s3_key_prefix(model_name, max_decision_id)

        # Request s3 keys belonging to that range
        s3_keys = list_s3_keys_containing(
            bucket_name     = os.environ['TRAIN_BUCKET'],
            start_after_key = start_after_key,
            end_key         = end_key,
            prefix          = f'/rewarded_decisions/{model_name}'
        )

        if len(s3_keys) == 0:
            return RewardedDecisionPartition(
                model_name, firehose_record_group.to_pandas_df()
            )


        # Example:
        # { s3_key1 : [ RDRs that belong to such parquet file ] }
        map_of_s3_keys_to_rdrs = defaultdict(list)

        for rdr in list(rdrs): # copy of rdrs list
            found_s3_file_for_rdr = False
            for i,s3_key in enumerate(s3_keys):
                
                min_9char_decision_id, max_9char_decision_id = \
                get_min_max_truncated_decision_ids_from_s3_key(s3_key)
                
                if decision_id_belongs_to_range(
                    rdr['decision_id'],
                    min_9char_decision_id,
                    max_9char_decision_id):

                        map_of_s3_keys_to_rdrs[s3_key].append(rdr) # ERROR: rdr has to be a FirehoseRecord
                        rdrs.remove(rdr)
                        found_s3_file_for_rdr = True
                        break
            
            if not found_s3_file_for_rdr:
                # What happens if I can't fit a RDR in one partition?
                # Maybe I should collect them all and return a Partition with all of them there
                pass

        # RewardedDecisionPartition for the records WITH a corresponding S3 key
        partitions_s3 = [
            RewardedDecisionPartition(
                model_name,
                FirehoseRecordGroup._to_pandas_df(map_of_s3_keys_to_rdrs[key]),
                s3_key=key
            ) for key in map_of_s3_keys_to_rdrs.keys()
        ]

        # RewardedDecisionPartition for the records WITHOUT a corresponding S3 key
        partitions_no_s3 = RewardedDecisionPartition(
            model_name,
            FirehoseRecordGroup._to_pandas_df(rdrs),
        )

        partitions_s3.append(partitions_no_s3)
        
        return partitions_s3
        

        # Plan: Do Plan1, then do some tests then do Plan2 and compare results

        # Plan 2
        #
        # Run each decision_id through the prefix function
        # You end up with '/rewarded_decisions/modelname/parquet/2014/05/13/000000000' for example
        # You also have s3 keys which look alike
        # Then you go through each s3 key and check if the fragment is part of the s3 key
        # But since the s3 keys are in lexicographic order, once



def get_min_max_truncated_decision_ids_from_s3_key(s3_key):
    """Extract the min and max truncated decision ids found in a S3 key str"""   
    
    regexp = r"/rewarded_decisions/.+/parquet/\d{4}/\d{2}/\d{2}/\w+-([A-Za-z0-9]{9})-\w+-([A-Za-z0-9]{9})-[A-Za-z0-9]{27}.parquet"
    result = re.match(regexp, s3_key)
    if result is not None:
        max_decision_id, min_decision_id = result.groups()
        return min_decision_id, max_decision_id
    
    raise ValueError('Problem with the format of the parquet file')


def min_max_decision_ids(partitions):
    min_decision_id = min(map(lambda x: x.min_decision_id, partitions))
    max_decision_id = max(map(lambda x: x.max_decision_id, partitions))
    return min_decision_id, max_decision_id


def repair_overlapping_keys(model_name, partitions):
    for partition in partitions:
        assert partition.model_name == model_name
        
    min_decision_id, max_decision_id = min_max_decision_ids(partitions)
    
    # TODO keep iterating until all overlapping keys have been repaired.  It may take multiple passes
    # TODO this is low priority for initial release since overlapping keys should be very rare
    
    return


def s3_key_prefix(model_name, max_decision_id):
    max_timestamp = Ksuid.from_base62(max_decision_id).datetime.strftime(ISO_8601_BASIC_FORMAT)
    
    yyyy = max_timestamp[0:4]
    mm = max_timestamp[4:6]
    dd = max_timestamp[6:8]
    
    #
    # Truncate the ksuid to 9 characters so that we're not exposing entire decision ids in the file names.
    # While having entire ids in the file names is likey no risk, it is also unneccessary.
    #
    # The max decision_id is encoded first in the path so that a lexicographically sorted
    # search of file names starting at the prefix of the target decision_id will provide
    # the .parquet that should contain that decision_id, if it exists
    #
    return f'/rewarded_decisions/{model_name}/parquet/{yyyy}/{mm}/{dd}/{max_timestamp}-{max_decision_id[:9]}'
    
    
def s3_key(model_name, min_decision_id, max_decision_id):
    min_timestamp = Ksuid.from_base62(min_decision_id).datetime.strftime(ISO_8601_BASIC_FORMAT)
    
    #
    # The min decision_id is encoded into the file name so that a lexicographically ordered listing
    # can determine if two parquet files have overlapping decision_id ranges, which they should not.
    # If overlapping ranges are detected they should be repaired by loading the overlapping parquet
    # files, consolidating them, optionally splitting, then saving.  This process should lead to
    # eventually consistency.
    #
    # The final KSUID is simply to give the file a random name.  We could have used UUIDv4 here
    # but we're already using KSUID, it's slightly shorter, and slightly easier to parse due
    # to no dashes.  For now, the characters following the last dash should be considered an opaque 
    # string of random characters
    #
    return f'{s3_key_prefix(model_name, max_decision_id)}-{min_timestamp}-{min_decision_id[:9]}-{Ksuid()}.parquet'
