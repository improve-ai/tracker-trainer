# TODO here are some hardcoded parquet names

# Built-in imports
import os
from pathlib import Path
import tempfile

# External imports
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
from partition import RewardedDecisionPartition, parquet_s3_key
from firehose_record import CONTEXT_KEY
from firehose_record import COUNT_KEY
from firehose_record import DECISION_ID_KEY
from firehose_record import DF_SCHEMA
from firehose_record import ITEM_KEY
from firehose_record import REWARD_KEY
from firehose_record import REWARDS_KEY
from firehose_record import SAMPLE_KEY
from tracker.tests_utils import dicts_to_df, get_valid_s3_key_from_df, get_model_name_from_env


ENGINE = "fastparquet"


"""
Tests asserting that saving and loading a Rewarded Decision Record to a Parquet file 
maintains the original RDR intact.

"""

class CasesMergeOfRewardedDecisions:

    def case_one_full_decision_one_partial(self, get_rewarded_decision_rec, get_partial_rewarded_dec_rec):

        return dicts_to_df(
            dicts=[get_rewarded_decision_rec(), get_partial_rewarded_dec_rec()],
            columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)


@parametrize_with_cases("rewarded_records_df", cases=CasesMergeOfRewardedDecisions)
def test_parquet(rewarded_records_df):

    rdp1 = RewardedDecisionPartition(get_model_name_from_env(), rewarded_records_df)
    rdp1.sort()
    rdp1.merge()

    valid_s3_key = get_valid_s3_key_from_df(df=rdp1.df, model_name=rdp1.model_name)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        # make sure parent dir exists
        (tmp_path / os.sep.join(valid_s3_key.split(os.sep)[:-1])).mkdir(parents=True, exist_ok=True)
        valid_s3_key_path = tmp_path / valid_s3_key

        rdp1.df.to_parquet(valid_s3_key_path, engine=ENGINE, index=False)
        restored = pd.read_parquet(valid_s3_key_path, engine=ENGINE)

        assert_frame_equal(restored, rdp1.df, check_column_type=True)
        pass


def test_parquet_types_unrewarded_rewarded_decision_record():

    # A rewarded decision record that has NOT been rewarded
    rdr1 = {
        DECISION_ID_KEY : "000000000000000000000000001",
        ITEM_KEY
         : '{ "text": "variant text" }',
        CONTEXT_KEY      : '{ "device" : "iPhone", "page" : 2462, "shared" : { "a": 1 } }',
        COUNT_KEY       : 1,
        SAMPLE_KEY      : '{ "text": "sample text" }',
    }

    # A rewarded decision record with all the possible missing values
    rdr2 = {
        DECISION_ID_KEY : "000000000000000000000000002",
        ITEM_KEY
         : '{}',
        CONTEXT_KEY      : '{}',
        COUNT_KEY       : 1,
        SAMPLE_KEY      :  '{}',
    }

    records = [rdr1, rdr2]

    df = dicts_to_df(dicts=records, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

    valid_s3_key = get_valid_s3_key_from_df(df=df, model_name=get_model_name_from_env())

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        # make sure parent dir exists
        (tmp_path / os.sep.join(valid_s3_key.split(os.sep)[:-1])).mkdir(parents=True, exist_ok=True)
        valid_s3_key_path = tmp_path / valid_s3_key

        df.to_parquet(valid_s3_key_path, engine=ENGINE, index=False)
        restored = pd.read_parquet(valid_s3_key_path, engine=ENGINE)
        assert_frame_equal(df, restored, check_column_type=True)


def test_parquet_types_rewarded_decision_record():
    # A rewarded decision record that HAS been rewarded
    rdr1 = {
        DECISION_ID_KEY : "000000000000000000000000001",
        ITEM_KEY
         : '{ "text": "variant text" }',
        CONTEXT_KEY      : '{ "device" : "iPhone", "page" : 2462, "shared" : { "a": 1 } }',
        COUNT_KEY       : 1,
        SAMPLE_KEY      : '{ "text": "sample text" }',
        REWARDS_KEY     : '{"000000000000000000000000001" : 1.2}',
        REWARD_KEY      : 1.2,
    }


    # The minimum possible rewarded decision record coming from a decision
    rdr2 = {
        DECISION_ID_KEY : "000000000000000000000000002",
        ITEM_KEY
         : '{}',
        CONTEXT_KEY      : '{}',
        COUNT_KEY       : 1,
        SAMPLE_KEY      : '{}',
    }

    records = [rdr1, rdr2]

    df = dicts_to_df(dicts=records, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

    valid_s3_key = get_valid_s3_key_from_df(df=df, model_name=get_model_name_from_env())

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        # make sure parent dir exists
        (tmp_path / os.sep.join(valid_s3_key.split(os.sep)[:-1])).mkdir(parents=True, exist_ok=True)
        valid_s3_key_path = tmp_path / valid_s3_key

        df.to_parquet(valid_s3_key_path, engine=ENGINE, index=False)
        restored = pd.read_parquet(valid_s3_key_path, engine=ENGINE)
        assert_frame_equal(df, restored, check_column_type=True)


def test_parquet_types_partial_rewarded_decision_record():
    # A partial rewarded decision record
    rdr1 = {
        DECISION_ID_KEY : "000000000000000000000000001",
        REWARDS_KEY     : '{"000000000000000000000000001" : 1.2}',
    }

    rdr2 = {
        DECISION_ID_KEY : "000000000000000000000000002",
    }

    records = [rdr1, rdr2]

    df = dicts_to_df(dicts=records, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    valid_s3_key = get_valid_s3_key_from_df(df=df, model_name=get_model_name_from_env())

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        # make sure parent dir exists
        (tmp_path / os.sep.join(valid_s3_key.split(os.sep)[:-1])).mkdir(parents=True, exist_ok=True)
        valid_s3_key_path = tmp_path / valid_s3_key

        df.to_parquet(valid_s3_key_path, engine=ENGINE, index=False)
        restored = pd.read_parquet(valid_s3_key_path, engine=ENGINE)

    assert_frame_equal(df, restored, check_column_type=True)

