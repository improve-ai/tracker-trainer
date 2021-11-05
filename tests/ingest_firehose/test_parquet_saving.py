# External imports
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
from firehose_record import FirehoseRecord
from rewarded_decisions import RewardedDecisionGroup
from test_merge_rewarded_decisions import fix_rewarded_decision_dict


class CasesMergeOfRewardedDecisions:

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

        rewarded_records_df = pd.concat([
            pd.DataFrame(rewarded_decision_rec),
            pd.DataFrame(partial_rewarded_rec)
        ], ignore_index=True)

        return rewarded_records_df



@parametrize_with_cases("rewarded_records_df, expected_df", cases=CasesMergeOfRewardedDecisions)
def test_parquet(rewarded_records_df, expected_df, tmp_path):
    """
    Test that a rewarded decision record saved to a Parquet file 
    and reading it back again produces the same rewarded decision 
    record.

    Parameters
    ----------
    tmp_path : Pathlib.Path
        Pytest fixture
    """

    temp_parquet_file = tmp_path / "temp.parquet"

    rdg1 = RewardedDecisionGroup("model_name", rewarded_records_df)
    rdg1.sort()
    rdg1.merge()

    rdg1.df.to_parquet(temp_parquet_file)
    restored = pd.read_parquet(temp_parquet_file)

    assert_frame_equal(restored, rdg1.df)
