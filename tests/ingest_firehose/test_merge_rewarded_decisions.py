# Built-in imports
import orjson as json

# External imports
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal
import dateutil 

# Local imports
from firehose_record import TIMESTAMP_KEY
from firehose_record import VARIANT_KEY
from firehose_record import GIVENS_KEY
from firehose_record import COUNT_KEY
from firehose_record import RUNNERS_UP_KEY
from firehose_record import SAMPLE_KEY
from firehose_record import REWARD_KEY
from firehose_record import REWARDS_KEY
from firehose_record import FirehoseRecord
from rewarded_decisions import RewardedDecisionGroup
from utils import utc

# TODO: 
# 1) Improve the get_record function to get fully compliant 
#    "decision" and "reward" records


def fix_rewarded_decision_dict(d):
    """ To be able to pass a dict into a Pandas DataFrame """

    if "rewards" in d and isinstance(d["rewards"], dict):
        d["rewards"] = [d["rewards"]]


def get_expected_rewarded_record(base_record, type_base_record, decision_id, rewards, reward):
    """
    Return a rewarded decision record based on the value of a decision 
    record but with some key values manually specified.
    """

    # Just as it's done in .to_rewarded_decision_dict
    # TODO: move that function to utils
    dumps = lambda x: json.dumps(x, option=json.OPT_SORT_KEYS).decode("utf-8")

    r = {
        "decision_id"  : decision_id,
    }

    if type_base_record == "decision":
    
        if TIMESTAMP_KEY in base_record:
            # parse and validate timestamp
            timestamp = dateutil.parser.parse(base_record[TIMESTAMP_KEY])
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=utc)
            
            r[TIMESTAMP_KEY] = timestamp
    
    else:
        # else: is a reward record and timesamp shouldn't be copied
        pass 

    if VARIANT_KEY in base_record:
        r[VARIANT_KEY] = dumps(base_record[VARIANT_KEY])
    
    if GIVENS_KEY in base_record:
        r[GIVENS_KEY] = dumps(base_record[GIVENS_KEY])

    if COUNT_KEY in base_record:
        r[COUNT_KEY] = float(base_record[COUNT_KEY])
    
    if RUNNERS_UP_KEY in base_record:
        r[RUNNERS_UP_KEY] = [dumps(x) for x in base_record[RUNNERS_UP_KEY]]
    
    if SAMPLE_KEY in base_record:
        r[SAMPLE_KEY] = dumps(base_record[SAMPLE_KEY])
    
    r[REWARDS_KEY] = [rewards] # List-wrapped for pandas only
    r[REWARD_KEY] = reward

    return r
    

class CasesMergeOfTwoRewardedDecisions:

    def case_one_full_decision_one_partial(self, get_record):
        
        decision_rec = get_record(
            type_val   = "decision",
            msg_id_val = "000000000000000000000000000"
        )

        rewarded_decision_rec = FirehoseRecord(decision_rec).to_rewarded_decision_dict()
        fix_rewarded_decision_dict(rewarded_decision_rec)

        reward_rec = get_record(
            type_val        = "reward",
            msg_id_val      = "111111111111111111111111111",
            decision_id_val = "000000000000000000000000000",
            reward_val      = -10
        )

        partial_rewarded_rec = FirehoseRecord(reward_rec).to_rewarded_decision_dict()
        fix_rewarded_decision_dict(partial_rewarded_rec)

        expected_rewarded_record = get_expected_rewarded_record(
            base_record      = decision_rec,
            type_base_record = "decision",
            decision_id      = "000000000000000000000000000",
            reward           = -10,
            rewards          = { "111111111111111111111111111" : -10 }
        )

        rewarded_records_df = pd.concat([
            pd.DataFrame(rewarded_decision_rec),
            pd.DataFrame(partial_rewarded_rec)
        ], ignore_index=True)

        expected_df = pd.DataFrame(expected_rewarded_record)

        return rewarded_records_df, expected_df


    def case_one_processed_full_decision_and_one_partial(self, get_record):

        decision_rec = get_record(
            type_val   = "decision",
            msg_id_val = "000000000000000000000000000"
        )
        
        rewarded_decision_rec = FirehoseRecord(decision_rec).to_rewarded_decision_dict()

        # Simulate that this record has already been merged with some rewards
        # The list wrapping the dict is needed for Pandas to receive a column
        rewarded_decision_rec["rewards"] = [{ "111111111111111111111111111" : -10 }]

        reward_rec = get_record(
            type_val        = "reward",
            msg_id_val      = "222222222222222222222222222",
            decision_id_val = "000000000000000000000000000",
            reward_val      = -10
        )

        partial_rewarded_decision_rec = FirehoseRecord(reward_rec).to_rewarded_decision_dict()
        fix_rewarded_decision_dict(partial_rewarded_decision_rec)

        expected_rewarded_record = get_expected_rewarded_record(
            base_record      = decision_rec,
            type_base_record = "decision",
            decision_id      = "000000000000000000000000000",
            reward           = -20,
            rewards          = {
                "111111111111111111111111111" : -10,
                "222222222222222222222222222" : -10
            }
        )

        dfs = pd.concat([
            pd.DataFrame(rewarded_decision_rec),
            pd.DataFrame(partial_rewarded_decision_rec)
        ], ignore_index=True)

        expected_df = pd.DataFrame(expected_rewarded_record)

        return dfs, expected_df


    def case_many_partial_rewarded_records(self, get_record):

        records = []
        for i in range(1, 6):
            
            reward_rec = get_record(
                type_val        = "reward",
                msg_id_val      = f"{i}"*27,
                decision_id_val = "000000000000000000000000000",
                reward_val      = i
            )

            partial_rewarded_decision_rec = FirehoseRecord(reward_rec).to_rewarded_decision_dict()
            fix_rewarded_decision_dict(partial_rewarded_decision_rec)
            
            records.append(pd.DataFrame(partial_rewarded_decision_rec))
        
        expected_rewarded_record = get_expected_rewarded_record(
            base_record      = reward_rec,
            type_base_record = "reward",
            decision_id      = "000000000000000000000000000",
            reward           = 15,
            rewards          = { 
                "111111111111111111111111111" : 1,
                "222222222222222222222222222" : 2,
                "333333333333333333333333333" : 3,
                "444444444444444444444444444" : 4,
                "555555555555555555555555555" : 5
            }
        )

        dfs = pd.concat(records, ignore_index=True)

        expected_df = pd.DataFrame(expected_rewarded_record)
        
        return dfs, expected_df


@parametrize_with_cases("rewarded_records_df, expected_df", cases=CasesMergeOfTwoRewardedDecisions)
def test_merge_two_rewarded_decisions_one_is_partial(rewarded_records_df, expected_df):

    rdg = RewardedDecisionGroup("model_name", rewarded_records_df)
    rdg.sort()
    rdg.merge()

    assert_frame_equal(rdg.df, expected_df)

