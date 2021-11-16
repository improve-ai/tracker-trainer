# External imports
from datetime import datetime
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
from rewarded_decisions import RewardedDecisionPartition
from firehose_record import assert_valid_rewarded_decision_record
from firehose_record import TIMESTAMP_KEY
from firehose_record import VARIANT_KEY
from firehose_record import GIVENS_KEY
from firehose_record import COUNT_KEY
from firehose_record import RUNNERS_UP_KEY
from firehose_record import SAMPLE_KEY
from firehose_record import REWARD_KEY
from firehose_record import REWARDS_KEY
from firehose_record import DECISION_ID_KEY
from firehose_record import to_pandas_df


ENGINE="fastparquet"

class CasesMergeOfRewardedDecisions:

    def case_one_full_decision_one_partial(self, rewarded_decision_rec, get_partial_rewarded_dec_rec):

        rewarded_records_df = pd.concat([
            to_pandas_df(rewarded_decision_rec),
            to_pandas_df([get_partial_rewarded_dec_rec()])
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

    rdp1 = RewardedDecisionPartition("model_name", rewarded_records_df)
    rdp1.sort()
    rdp1.merge()

    rdp1.df.to_parquet(temp_parquet_file, engine=ENGINE, index=False)
    restored = pd.read_parquet(temp_parquet_file, engine=ENGINE)

    assert_frame_equal(restored, rdp1.df, check_column_type=True)


def test_parquet_types_unrewarded_rewarded_decision_record(tmp_path):

    temp_parquet_file = tmp_path / "temp.parquet"

    # A rewarded decision record that has NOT been rewarded
    rdr1 = {
        DECISION_ID_KEY : "000000000000000000000000001",
        TIMESTAMP_KEY   : datetime.fromisoformat("2021-10-07T07:24:06.126+02:00"),
        VARIANT_KEY     : '{ "text": "variant text" }',
        GIVENS_KEY      : '{ "device" : "iPhone", "page" : 2462, "shared" : { "a": 1 } }',
        COUNT_KEY       : 1,
        RUNNERS_UP_KEY  : '[{ "text": "You are safe." }, { "text": "You are safe." }]',
        SAMPLE_KEY      : '{ "text": "sample text" }',
    }
    assert_valid_rewarded_decision_record(rdr1, record_type="decision")

    # A rewarded decision record with all the possible missing values
    rdr2 = {
        DECISION_ID_KEY : "000000000000000000000000002",
        TIMESTAMP_KEY   : datetime.fromisoformat("2021-10-07T07:24:06.126+02:00"),
        VARIANT_KEY     : '{}',
        GIVENS_KEY      : '{}',
        COUNT_KEY       : 1,
        # RUNNERS_UP_KEY  : must not be set when missing,
        SAMPLE_KEY      :  '{}',
    }
    assert_valid_rewarded_decision_record(rdr2, record_type="decision")

    records = [rdr1, rdr2]

    df = to_pandas_df(records)
    df.to_parquet(temp_parquet_file, engine=ENGINE, index=False) 
    restored = pd.read_parquet(temp_parquet_file, engine=ENGINE)
    assert_frame_equal(df, restored, check_column_type=True)


def test_parquet_types_rewarded_decision_record(tmp_path):
    temp_parquet_file = tmp_path / "temp.parquet"

    # A rewarded decision record that HAS been rewarded
    rdr1 = {
        DECISION_ID_KEY : "000000000000000000000000001",
        TIMESTAMP_KEY   : datetime.fromisoformat("2021-10-07T07:24:06.126+02:00"),
        VARIANT_KEY     : '{ "text": "variant text" }',
        GIVENS_KEY      : '{ "device" : "iPhone", "page" : 2462, "shared" : { "a": 1 } }',
        COUNT_KEY       : 1,
        RUNNERS_UP_KEY  : '[{ "text": "You are safe." }, { "text": "You are safe." }]',
        SAMPLE_KEY      : '{ "text": "sample text" }',
        REWARDS_KEY     : '{"000000000000000000000000001" : 1.2}',
        REWARD_KEY      : 1.2,
    }

    assert_valid_rewarded_decision_record(rdr1, record_type="decision")

    # The minimum possible rewarded decision record coming from a decision
    rdr2 = {
        DECISION_ID_KEY : "000000000000000000000000002",
        TIMESTAMP_KEY   : datetime.fromisoformat("2021-10-07T07:24:06.126+02:00"),
        VARIANT_KEY     : '{}',
        GIVENS_KEY      : '{}',
        COUNT_KEY       : 1,
        # RUNNERS_UP_KEY  : must not be set when missing,
        SAMPLE_KEY      : '{}',
    }
    assert_valid_rewarded_decision_record(rdr2, record_type="decision")

    records = [rdr1, rdr2]

    df = to_pandas_df(records)
    df.to_parquet(temp_parquet_file, engine=ENGINE, index=False)
    restored = pd.read_parquet(temp_parquet_file, engine=ENGINE)
    assert_frame_equal(df, restored, check_column_type=True)


def test_parquet_types_partial_rewarded_decision_record(tmp_path):
    """
    """

    temp_parquet_file = tmp_path / "temp.parquet"

    # A partial rewarded decision record 
    rdr1 = {
        DECISION_ID_KEY : "000000000000000000000000001",
        REWARDS_KEY     : '{"000000000000000000000000001" : 1.2}',
    }
    assert_valid_rewarded_decision_record(rdr1, record_type="reward")

    rdr2 = {
        DECISION_ID_KEY : "000000000000000000000000002",
    }
    assert_valid_rewarded_decision_record(rdr2, record_type="reward")

    records = [rdr1, rdr2]

    df = to_pandas_df(records)
    df.to_parquet(temp_parquet_file, engine=ENGINE, index=False)
    restored = pd.read_parquet(temp_parquet_file, engine=ENGINE)

    assert_frame_equal(df, restored, check_column_type=True)

