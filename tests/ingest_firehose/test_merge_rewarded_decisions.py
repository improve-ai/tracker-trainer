# External imports
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
from rewarded_decisions import RewardedDecisionPartition
from firehose_record import DF_SCHEMA, REWARD_KEY, REWARDS_KEY, TYPE_KEY
from firehose_record import FirehoseRecordGroup, FirehoseRecord, assert_valid_rewarded_decision_record
from utils import json_dumps

from tests.ingest_firehose.utils import dicts_to_df

# TODO: start at jsons and end up merging

class CasesMergeOfRewardedDecisions:
    """

    The possible fixtures that the cases can receive are the following:

    get_rewarded_decision_rec: dict
        Instance of a "rewarded decision record" built from a 
        single "decision" record
    
    partial_rewarded_dec_rec: function
        To get a partial "rewarded decision record" built from a single 
        "reward" record
    
    helpers: Helpers
        Class of custom useful static methods
    """

    def case_one_full_decision_one_partial(self, 
        get_rewarded_decision_rec, get_partial_rewarded_dec_rec, get_decision_rec, helpers):

        record = get_decision_rec()
        expected_rewarded_record = FirehoseRecord(record).to_rewarded_decision_dict()
        expected_rewarded_record[REWARD_KEY] = -10
        expected_rewarded_record[REWARDS_KEY] = json_dumps({ "000000000000000000000000001" : -10 })
        assert_valid_rewarded_decision_record(expected_rewarded_record, record[TYPE_KEY])

        rewarded_records_df = dicts_to_df(
            dicts=[get_rewarded_decision_rec(), get_partial_rewarded_dec_rec()],
            columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

        expected_df = dicts_to_df(
            dicts=[expected_rewarded_record], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

        return rewarded_records_df, expected_df


    def case_one_processed_full_rewarded_dec_rec_and_one_partial_rewarded_dec_rec(self, 
        get_decision_rec, get_rewarded_decision_rec, get_partial_rewarded_dec_rec, helpers):

        rdr = get_rewarded_decision_rec()
        # Simulate that this record has already been merged with some rewards
        rdr["rewards"] = '{ "000000000000000000000000001" : -10 }'

        # A partial rewarded decicion record with a different message_id value
        partial_rewarded_dec_rec = get_partial_rewarded_dec_rec(msg_id_val="000000000000000000000000002")

        # The expected RDR
        record = get_decision_rec()
        expected_rewarded_record = FirehoseRecord(record).to_rewarded_decision_dict()
        expected_rewarded_record[REWARD_KEY] = -20
        expected_rewarded_record[REWARDS_KEY] = json_dumps({
            "000000000000000000000000001" : -10,
            "000000000000000000000000002" : -10
        })
        assert_valid_rewarded_decision_record(expected_rewarded_record, record[TYPE_KEY])

        dfs = dicts_to_df(
            dicts=[rdr, partial_rewarded_dec_rec], columns=DF_SCHEMA.keys(),
            dtypes=DF_SCHEMA)

        expected_df = dicts_to_df(
            dicts=[expected_rewarded_record], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

        return dfs, expected_df


    def case_many_partial_rewarded_records(self, get_record, get_reward_rec, helpers):

        records = []
        for i in range(1, 6):
            
            reward_rec = get_record(
                type_val        = "reward",
                msg_id_val      = f"00000000000000000000000000{i}",
                decision_id_val = "000000000000000000000000000",
                reward_val      = i
            )

            partial_rewarded_decision_rec = helpers.to_rewarded_decision_record(reward_rec)

            records.append(
                dicts_to_df(dicts=[partial_rewarded_decision_rec],
                            columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA))

        record = get_reward_rec()
        expected_rewarded_record = FirehoseRecord(record).to_rewarded_decision_dict()
        expected_rewarded_record[REWARD_KEY] = 15
        expected_rewarded_record[REWARDS_KEY] = json_dumps({ 
            "000000000000000000000000001" : 1,
            "000000000000000000000000002" : 2,
            "000000000000000000000000003" : 3,
            "000000000000000000000000004" : 4,
            "000000000000000000000000005" : 5
        })
        assert_valid_rewarded_decision_record(expected_rewarded_record, record[TYPE_KEY])


        dfs = pd.concat(records, ignore_index=True)

        expected_df = \
            dicts_to_df(dicts=[expected_rewarded_record], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
        
        return dfs, expected_df


    def case_duplicated_reward_records(self, get_record, get_reward_rec, helpers):

        dup_records = []
        for i in [3, 3, 3, 3, 3]:
            
            reward_rec = get_record(
                type_val        = "reward",
                msg_id_val      = f"00000000000000000000000000{i}",
                decision_id_val = "000000000000000000000000000",
                reward_val      = i
            )

            partial_rewarded_decision_rec = helpers.to_rewarded_decision_record(reward_rec)
            dup_records.append(
                dicts_to_df(dicts=[partial_rewarded_decision_rec],
                            columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA))

        record = get_reward_rec()
        expected_rewarded_record = FirehoseRecord(record).to_rewarded_decision_dict()
        expected_rewarded_record[REWARD_KEY] = 3
        expected_rewarded_record[REWARDS_KEY] = json_dumps({ "000000000000000000000000003" : 3 })
        assert_valid_rewarded_decision_record(expected_rewarded_record, record[TYPE_KEY])

        dfs = pd.concat(dup_records, ignore_index=True)
        expected_df = \
            dicts_to_df(dicts=[expected_rewarded_record], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
        
        return dfs, expected_df


    def case_same_rewarded_decision_records_with_no_reward(self, get_decision_rec, helpers):


        decision_record = get_decision_rec()

        # Even though the name says "rewarded", has no rewards in it
        rewarded_decision_record1 = helpers.to_rewarded_decision_record(decision_record)
        rewarded_decision_record2 = helpers.to_rewarded_decision_record(decision_record)       
        
        assert rewarded_decision_record1.get(REWARDS_KEY) is None


        record = get_decision_rec()
        expected_rewarded_record = FirehoseRecord(record).to_rewarded_decision_dict()
        expected_rewarded_record[REWARD_KEY] = 0
        expected_rewarded_record[REWARDS_KEY] = json_dumps({})
        assert_valid_rewarded_decision_record(expected_rewarded_record, record[TYPE_KEY])

        dfs = dicts_to_df(
            dicts=[rewarded_decision_record1, rewarded_decision_record2],
            columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

        expected_df = \
            dicts_to_df(dicts=[expected_rewarded_record], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
        
        return dfs, expected_df



@parametrize_with_cases("rewarded_records_df, expected_df", cases=CasesMergeOfRewardedDecisions)
def test_merge_of_rewarded_decision_records(rewarded_records_df, expected_df):

    rdg = RewardedDecisionPartition("some_model_name", rewarded_records_df)
    rdg.sort()
    rdg.merge()

    assert_frame_equal(rdg.df, expected_df, check_column_type=True)
    


@parametrize_with_cases("rewarded_records_df, expected_df", cases=CasesMergeOfRewardedDecisions)
def test_idempotency1(rewarded_records_df, expected_df):
    """
    Test idempotency by running the same process twice "in parallel", 
    merging the results and running it again.
    """

    rdg1 = RewardedDecisionPartition("model_name", rewarded_records_df)
    rdg1.sort()
    rdg1.merge()

    rdg2 = RewardedDecisionPartition("model_name", rewarded_records_df)
    rdg2.sort()
    rdg2.merge()

    parallel_df = pd.concat([rdg1.df, rdg2.df], ignore_index=True)

    rdg3 = RewardedDecisionPartition("model_name", parallel_df)
    rdg3.sort()
    rdg3.merge()

    assert_frame_equal(rdg3.df, expected_df, check_column_type=True)
