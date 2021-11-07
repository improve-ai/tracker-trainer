# External imports
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
from rewarded_decisions import RewardedDecisionPartition


class CasesMergeOfRewardedDecisions:

    def case_one_full_decision_one_partial(self, rewarded_dec_rec, get_partial_rewarded_dec_rec):

        rewarded_records_df = pd.concat([
            pd.DataFrame(rewarded_dec_rec),
            pd.DataFrame(get_partial_rewarded_dec_rec())
        ], ignore_index=True)

        return rewarded_records_df


@parametrize_with_cases("rewarded_records_df", cases=CasesMergeOfRewardedDecisions)
def test_parquet(rewarded_records_df, tmp_path):
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

    rdg1 = RewardedDecisionPartition("model_name", rewarded_records_df)
    rdg1.sort()
    rdg1.merge()

    rdg1.df.to_parquet(temp_parquet_file)
    restored = pd.read_parquet(temp_parquet_file)

    assert_frame_equal(restored, rdg1.df)
