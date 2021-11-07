# Built-in imports
from copy import deepcopy

# External imports
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
from rewarded_decisions import RewardedDecisionPartition
from firehose_record import REWARDS_KEY


# TODO: start at jsons and end up merging

class CasesMergeOfRewardedDecisions:
    """

    The possible fixtures that the cases can receive are the following:

    dec_rec : dict
        Instance of a "decision" record
    
    rewarded_dec_rec: dict
        Instance of a "rewarded decision record" built from a 
        single "decision" record
    
    partial_rewarded_dec_rec: function
        To get a partial "rewarded decision record" built from a single 
        "reward" record
    
    helpers: Helpers
        Class of custom useful static methods
    """

    def case_one_full_decision_one_partial(self, 
        dec_rec, rewarded_dec_rec, get_partial_rewarded_dec_rec, helpers):

        expected_rewarded_record = helpers.get_expected_rewarded_record(
            base_record      = dec_rec(),
            type_base_record = "decision",
            decision_id      = "000000000000000000000000000",
            reward           = -10,
            rewards          = { "000000000000000000000000001" : -10 }
        )

        rewarded_records_df = pd.concat([
            pd.DataFrame(rewarded_dec_rec),
            pd.DataFrame(get_partial_rewarded_dec_rec())
        ], ignore_index=True)

        expected_df = pd.DataFrame(expected_rewarded_record)

        return rewarded_records_df, expected_df


    def case_one_processed_full_rewarded_dec_rec_and_one_partial_rewarded_dec_rec(self, 
        dec_rec, rewarded_dec_rec, get_partial_rewarded_dec_rec, helpers):

        # Simulate that this record has already been merged with some rewards
        # The list wrapping the dict is needed for Pandas to receive a column
        rewarded_dec_rec["rewards"] = [{ "000000000000000000000000001" : -10 }]

        # A partial rewarded decicion record with a different message_id value
        partial_rewarded_dec_rec = get_partial_rewarded_dec_rec(msg_id_val="000000000000000000000000002")

        expected_rewarded_record = helpers.get_expected_rewarded_record(
            base_record      = dec_rec(),
            type_base_record = "decision",
            decision_id      = "000000000000000000000000000",
            reward           = -20,
            rewards          = {
                "000000000000000000000000001" : -10,
                "000000000000000000000000002" : -10
            }
        )
        
        dfs = pd.concat([
            pd.DataFrame(rewarded_dec_rec),
            pd.DataFrame(partial_rewarded_dec_rec)
        ], ignore_index=True)

        expected_df = pd.DataFrame(expected_rewarded_record)

        return dfs, expected_df


    def case_many_partial_rewarded_records(self, get_record, helpers):

        records = []
        for i in range(1, 6):
            
            reward_rec = get_record(
                type_val        = "reward",
                msg_id_val      = f"00000000000000000000000000{i}",
                decision_id_val = "000000000000000000000000000",
                reward_val      = i
            )

            partial_rewarded_decision_rec = helpers.to_rewarded_decision_record(reward_rec)

            records.append(pd.DataFrame(partial_rewarded_decision_rec))
        
        expected_rewarded_record = helpers.get_expected_rewarded_record(
            base_record      = reward_rec,
            type_base_record = "reward",
            decision_id      = "000000000000000000000000000",
            reward           = 15,
            rewards          = { 
                "000000000000000000000000001" : 1,
                "000000000000000000000000002" : 2,
                "000000000000000000000000003" : 3,
                "000000000000000000000000004" : 4,
                "000000000000000000000000005" : 5
            }
        )

        dfs = pd.concat(records, ignore_index=True)

        expected_df = pd.DataFrame(expected_rewarded_record)
        
        return dfs, expected_df


    def case_duplicated_reward_records(self, get_record, helpers):

        dup_records = []
        for i in [3, 3, 3, 3, 3]:
            
            reward_rec = get_record(
                type_val        = "reward",
                msg_id_val      = f"00000000000000000000000000{i}",
                decision_id_val = "000000000000000000000000000",
                reward_val      = i
            )

            partial_rewarded_decision_rec = helpers.to_rewarded_decision_record(reward_rec)
            dup_records.append(pd.DataFrame(partial_rewarded_decision_rec))

        expected_rewarded_record = helpers.get_expected_rewarded_record(
            base_record      = reward_rec,
            type_base_record = "reward",
            decision_id      = "000000000000000000000000000",
            reward           = 3,
            rewards          = { 
                "000000000000000000000000003" : 3,
            }
        )

        dfs = pd.concat(dup_records, ignore_index=True)
        expected_df = pd.DataFrame(expected_rewarded_record)
        
        return dfs, expected_df


    def case_same_rewarded_decision_records_with_no_reward(self, dec_rec, helpers):
        """

        """

        decision_record = dec_rec()

        # Even though the name says "rewarded", has no rewards in it
        rewarded_decision_record1 = helpers.to_rewarded_decision_record(decision_record)
        rewarded_decision_record2 = helpers.to_rewarded_decision_record(decision_record)
        
        assert rewarded_decision_record1.get(REWARDS_KEY) is None

        expected_rewarded_record = helpers.get_expected_rewarded_record(
            base_record      = decision_record,
            type_base_record = "decision",
            decision_id      = "000000000000000000000000000"
        )

        dfs = pd.concat([
            pd.DataFrame(rewarded_decision_record1),
            pd.DataFrame(rewarded_decision_record2)
        ], ignore_index=True)
        expected_df = pd.DataFrame(expected_rewarded_record)
        
        return dfs, expected_df

    # def case_distinct_rewarded_decision_records_with_no_reward(self, dec_rec, helpers): 
    #     pass


@parametrize_with_cases("rewarded_records_df, expected_df", cases=CasesMergeOfRewardedDecisions)
def test_merge_of_rewarded_decision_records(rewarded_records_df, expected_df):

    rdg = RewardedDecisionPartition("some_model_name", rewarded_records_df)
    rdg.sort()
    rdg.merge()

    assert_frame_equal(rdg.df, expected_df)
    


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

    assert_frame_equal(rdg3.df, expected_df)
