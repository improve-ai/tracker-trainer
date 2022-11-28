# External imports
import orjson
import os
from pytest_cases import parametrize_with_cases
import pandas as pd
from pandas.testing import assert_frame_equal

# Local imports
import src.ingest.config

src.ingest.config.FIREHOSE_BUCKET = os.getenv('FIREHOSE_BUCKET', None)
assert src.ingest.config.FIREHOSE_BUCKET is not None

src.ingest.config.TRAIN_BUCKET = os.getenv('TRAIN_BUCKET', None)
assert src.ingest.config.TRAIN_BUCKET is not None

import src.ingest.firehose_record
from src.ingest.firehose_record import DF_SCHEMA, REWARD_KEY, REWARDS_KEY,\
    TYPE_KEY, FirehoseRecordGroup, FirehoseRecord, assert_valid_rewarded_decision_record
import src.ingest.partition
from src.ingest.partition import RewardedDecisionPartition
import src.ingest.utils
from src.ingest.utils import json_dumps

from tests_utils import dicts_to_df, upload_gzipped_records_to_firehose_bucket

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
        rdr["rewards"] = '{"000000000000000000000000001":-10}'

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


def unpack_merge_test_case(test_case_file: str):
    test_case_path = os.sep.join([os.getenv('TEST_CASES_DIR'), test_case_file])
    with open(test_case_path, 'r') as tcf:
        test_case_json = orjson.loads(tcf.read())
    return test_case_json


def prepare_moto_deps(s3_client, firehose_bucket_file=None, train_bucket_files=None):
    # create firehose bucket
    s3_client.create_bucket(Bucket=src.ingest.config.FIREHOSE_BUCKET)

    test_cases_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_cases_dir is not None

    merge_test_cases_relative_dir = os.getenv('MERGE_TEST_CASES_RELATIVE_DIR', None)
    assert merge_test_cases_relative_dir is not None

    if firehose_bucket_file is not None:
        firehose_bucket_file_path = \
            os.sep.join([test_cases_dir, merge_test_cases_relative_dir, firehose_bucket_file])
        # firehose_file_s3_key = f's3://{src.ingest.config.FIREHOSE_BUCKET/{firehose_bucket_file}'
        upload_gzipped_records_to_firehose_bucket(
            s3_client=s3_client, path=firehose_bucket_file_path, key=firehose_bucket_file)

    # create train bucket
    s3_client.create_bucket(Bucket=src.ingest.config.TRAIN_BUCKET)
    if train_bucket_files is not None:
        for train_bucket_file in train_bucket_files:
            train_bucket_file_path = os.sep.join(
                [test_cases_dir, merge_test_cases_relative_dir, train_bucket_file])
            # read parquet if provided
            full_parquet_s3_key = f's3://{src.ingest.config.TRAIN_BUCKET}/{train_bucket_file}'
            # call to_parquet() specifying proper S3 key
            pd.read_parquet(train_bucket_file_path).to_parquet(full_parquet_s3_key, compression='ZSTD', index=False)


def get_expected_outputs(expected_output_files):
    test_cases_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_cases_dir is not None

    merge_test_cases_relative_dir = os.getenv('MERGE_TEST_CASES_RELATIVE_DIR', None)
    assert merge_test_cases_relative_dir is not None

    expected_outputs = []
    for expected_output_file in expected_output_files:
        expected_output_path = \
            os.sep.join([test_cases_dir, merge_test_cases_relative_dir, expected_output_file])
        expected_outputs.append(pd.read_parquet(expected_output_path).astype(DF_SCHEMA))

    return expected_outputs


# TODO add multiple decision model merge test case!!!
def _generic_merge_test_case(test_case_file, s3):

    test_case_json = unpack_merge_test_case(test_case_file=test_case_file)
    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    gzipped_records = test_case.get('gzipped_records', None)
    assert gzipped_records is not None

    merged_s3_keys = test_case.get('merged_s3_keys', None)
    # assert merged_s3_keys is not None

    model_names = test_case.get('model_names', None)
    assert model_names is not None

    # Patch the s3 client used in list_delimited_s3_keys
    src.ingest.config.s3client = src.ingest.utils.s3client = \
        src.ingest.partition.s3client = src.ingest.firehose_record.s3client = s3

    # upload all dependencies to mocked s3
    prepare_moto_deps(
        s3_client=s3, firehose_bucket_file=gzipped_records,
        train_bucket_files=merged_s3_keys)

    # create record groups
    record_groups = FirehoseRecordGroup.load_groups(s3_key=gzipped_records)

    # create model_name: <s3 key> map
    if merged_s3_keys is not None:
        model_name_to_parquet_key_map = dict(zip(model_names, merged_s3_keys))

    # prepare partitions from record groups
    partitions = [
        RewardedDecisionPartition(
            rg.model_name, rg.to_pandas_df(),
            s3_keys=[model_name_to_parquet_key_map[rg.model_name]] if merged_s3_keys is not None else None)
        for rg in record_groups]

    for p in partitions:
        p.load()
        p.sort()
        p.merge()

    # compare results with expected parquet files
    # load expected outputs
    expected_outputs_files = test_case_json.get('expected_outputs_files', None)
    assert expected_outputs_files is not None

    expected_outputs = get_expected_outputs(expected_outputs_files)
    expected_outputs_map = dict(zip(model_names, expected_outputs))

    # result of each partition must be equal to corresponding expected output
    for p in partitions:
        pd.testing.assert_frame_equal(p.df, expected_outputs_map[p.model_name])


# test merge with initial batch jsonlines
def test_initial_batch_merge_single_model(s3):
    test_case_file = os.getenv('TEST_SINGLE_MODEL_MERGE_INITIAL_BATCH_JSON', None)
    assert test_case_file is not None
    _generic_merge_test_case(test_case_file, s3)


def test_additional_rewards_batch_merge_single_model(s3):
    test_case_file = os.getenv('TEST_SINGLE_MODEL_MERGE_INITIAL_BATCH_AND_ADDITIONAL_REWARDS_BATCH_JSON', None)
    assert test_case_file is not None
    _generic_merge_test_case(test_case_file, s3)


def test_only_additional_rewards_batch_merge_single_model(s3):
    test_case_file = os.getenv('TEST_SINGLE_MODEL_MERGE_ONLY_ADDITIONAL_REWARDS_BATCH_JSON', None)
    assert test_case_file is not None
    _generic_merge_test_case(test_case_file, s3)


def test_initial_batch_merge_multiple_models(s3):
    test_case_file = os.getenv('TEST_MULTIPLE_MODELS_MERGE_INITIAL_BATCH_JSON', None)
    assert test_case_file is not None
    _generic_merge_test_case(test_case_file, s3)


def test_additional_rewards_batch_merge_multiple_models(s3):
    test_case_file = os.getenv('TEST_MULTIPLE_MODELS_MERGE_INITIAL_BATCH_AND_ADDITIONAL_REWARDS_BATCH_JSON', None)
    assert test_case_file is not None
    _generic_merge_test_case(test_case_file, s3)


def test_only_additional_rewards_batch_merge_multiple_models(s3):
    test_case_file = os.getenv('TEST_MULTIPLE_MODELS_MERGE_ONLY_ADDITIONAL_REWARDS_BATCH_JSON', None)
    assert test_case_file is not None
    _generic_merge_test_case(test_case_file, s3)
