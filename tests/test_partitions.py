import gzip
import numpy as np
import orjson
import os
from pytest import raises

import pandas as pd
from firehose_record import DF_SCHEMA, FirehoseRecord, FirehoseRecordGroup, \
    DECISION_ID_KEY, MESSAGE_ID_KEY, TYPE_KEY
from partition import RewardedDecisionPartition, parquet_s3_key_prefix
from tests_utils import dicts_to_df, load_ingest_test_case


def upload_rdrs_as_parquet_files_to_train_bucket(rdrs, model_name):
    """ """

    assert isinstance(model_name, str)

    # Create df based on the rdrs
    df = dicts_to_df(dicts=rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    RDP = RewardedDecisionPartition(model_name=model_name, df=df)
    RDP.sort()
    RDP.save()


def get_rdrs_before_or_after_decision_id(
    firehose_record_group,
    pivot1,
    op1,
    pivot2=None,
    op2=None):
    """
    Convert records from a Firehose Record Group into RDRs and return a subset
    of them depending if they are "<=" or ">" than a pivot record.
    """
    
    expected_rdrs = []
    for fhr in firehose_record_group.records:
        
        rdr = fhr.to_rewarded_decision_dict()

        if fhr.is_decision_record():
            if op1(fhr.message_id, pivot1):
                if op2 is None and pivot2 is None:
                    expected_rdrs.append(rdr)
                else:
                    if op2(fhr.message_id, pivot2):
                        expected_rdrs.append(rdr)

        elif fhr.is_reward_record():
            if op1(fhr.decision_id, pivot1):
                if op2 is None and pivot2 is None:
                    expected_rdrs.append(rdr)
                else:
                    if op2(fhr.decision_id, pivot2):
                        expected_rdrs.append(rdr)
                    else:
                        print(f"Passing from: {fhr.message_id}")
    
    expected_rdrs.sort(key=lambda rdr: rdr[DECISION_ID_KEY])
    df = dicts_to_df(dicts=expected_rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

    return df


def assert_only_last_partition_may_not_have_an_s3_key(rdps):
    if len(rdps) > 1:
        assert all(rdp.s3_key is not None for rdp in rdps[:-1])


def _get_records_array_from_gzipped_records_file(records_file):

    test_case_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_case_dir is not None

    merge_test_data_relative_dir = os.getenv('MERGE_TEST_DATA_RELATIVE_DIR', None)
    assert merge_test_data_relative_dir is not None

    records_path = \
        os.sep.join([test_case_dir, merge_test_data_relative_dir, records_file])

    # open jsonlines file
    with gzip.GzipFile(records_path) as gzipped_jls:
        jsonlines = [orjson.loads(jl) for jl in gzipped_jls.readlines()]

    # prepare jsonlines
    for jl in jsonlines:
        if jl[TYPE_KEY] == 'decision':
            jl[DECISION_ID_KEY] = jl[MESSAGE_ID_KEY]

    records_jsonlines = \
        [FirehoseRecord(jl).to_rewarded_decision_dict() for jl in jsonlines]
    return pd.DataFrame(records_jsonlines, columns=DF_SCHEMA.keys()).astype(DF_SCHEMA).values


def _get_records_not_nan_mask(records):
    records[records == None] = np.nan
    nans_filtering_container = records * np.full(records.shape, 0)
    return (nans_filtering_container == '') + (nans_filtering_container == 0)


def _load_expected_records_parquet(expected_output_parquet_file):
    test_case_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_case_dir is not None

    merge_test_data_relative_dir = os.getenv('MERGE_TEST_DATA_RELATIVE_DIR',
                                             None)
    assert merge_test_data_relative_dir is not None

    expected_output_path = \
        os.sep.join(
            [test_case_dir, merge_test_data_relative_dir, expected_output_parquet_file])

    expected_records = pd.read_parquet(expected_output_path).astype(DF_SCHEMA).values
    expected_records[expected_records == None] = np.nan
    return expected_records


def _process_tested_records(records):
    # if records point to *.gz file read gzipped file
    # if records stores a list assume that this is a list of records
    if isinstance(records, str) and records.endswith('.gz'):
        # read gzipped records
        # open jsonlines file
        records = _get_records_array_from_gzipped_records_file(records_file=records)
    elif isinstance(records, list):
        records = np.array(records, dtype=object)
    else:
        raise ValueError(f'Unsupported records: {records}')

    return records


def _process_expected_output(expected_output):
    # if this is a string and ends with .parquet then load it
    if isinstance(expected_output, str) and expected_output.endswith(
            '.parquet'):
        expected_output = _load_expected_records_parquet(expected_output)
    elif isinstance(expected_output, list):
        expected_output = \
            pd.DataFrame(np.array(expected_output, dtype=object), columns=DF_SCHEMA.keys()) \
            .astype(DF_SCHEMA).values
        expected_output[expected_output == None] = np.nan
    else:
        raise ValueError(f'Unsupported expected_output: {expected_output}')

    return expected_output


def _generic_test__merge_many_records_group(test_case_file: str):
    test_case_json = load_ingest_test_case(test_case_file=test_case_file)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    records = test_case.get("records", None)
    assert records is not None
    records = _process_tested_records(records)

    group_slice_start = test_case.get('group_slice_start', None)
    assert group_slice_start is not None

    group_slice_end = test_case.get('group_slice_end', None)
    assert group_slice_end is not None

    model_name = test_case.get("model_name", None)
    assert model_name is not None

    p = RewardedDecisionPartition(model_name=model_name)
    merged_records = np.full((1, records.shape[1]), np.nan, dtype=object)
    records_not_nan_mask = _get_records_not_nan_mask(records)

    p._merge_many_records_group(
        records_array=records, array_not_nans_mask=records_not_nan_mask,
        group_slice_start=group_slice_start, group_slice_end=group_slice_end,
        into=merged_records[0])
    expected_output = test_case_json.get('expected_output', None)
    assert expected_output is not None
    expected_output = _process_expected_output(expected_output)

    merged_records_desired_dtypes = \
        pd.DataFrame(merged_records, columns=DF_SCHEMA.keys())\
        .astype(DF_SCHEMA).values

    for cv, ev in zip(merged_records_desired_dtypes[0], expected_output[0]):
        print(f'cv: {cv} (type: {type(cv)}) | ev: {cv} (type: {type(ev)}) | equal: {cv == ev}')

    merged_df = \
        pd.DataFrame(merged_records, columns=DF_SCHEMA).astype(DF_SCHEMA)
    expected_df = \
        pd.DataFrame(expected_output, columns=DF_SCHEMA.keys()).astype(DF_SCHEMA)
    # TODO pandas assert frame equal passes but numpy wont allow
    #  np.nan == np.nan of dtype object to pass
    pd.testing.assert_frame_equal(merged_df, expected_df)

    # a = np.array([1, 2, np.nan, np.nan], dtype=object)
    # b = np.array([1, 2, np.nan, np.nan], dtype=object)
    # print(np.array_equal(a, b, equal_nan=False))
    # np.testing.assert_array_equal(a, b)
    # np.testing.assert_array_equal(merged_records_desired_dtypes, expected_output)


def test__merge_many_records_group_1():
    # This case tests a scenario in which a group consist of 1 valid decision
    # record and 2 valid reward records
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_1_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_2():
    # This case tests a scenario in which a group consist of 2 decision
    # records (from those 2 first not nan elements should be selected)
    # and 2 valid reward records
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_2_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_3():
    # This case tests a scenario in which a group consist of 2 decision
    # records (from those 2 first not nan elements should be selected)
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_3_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_4():
    # This case tests a scenario in which a group consist of 2 rewards records
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_4_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_5():
    # This case tests a scenario in which a group consist of 2 rewards records
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_5_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_6():
    # This case tests a scenario in which a group consist of 2 'decision records' and
    # 2 reward records with nulls in "rewards" and "reward" columns
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_6_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_7():
    # This case tests a scenario in which a group consist of 2 'decision records' and
    # 2 reward records of which 1 with nulls in "rewards" and "reward" columns and
    # second with "{}" in rewards and 0.0 in reward column
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_7_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_8():
    # This case tests a scenario in which a group
    # - 2 'decision records'
    # - 1 partial reward record (already merged)
    # - 1 new reward record
    test_case_file = os.getenv('TEST__MERGE_MANY_RECORDS_GROUP_8_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_many_records_group(test_case_file=test_case_file)


def test__merge_many_records_group_raises_for_bad_slice():
    raising_test_cases = \
        [f'TEST__MERGE_MANY_RECORDS_GROUP_{i}_JSON' for i in range(9, 14)]

    for test_case in raising_test_cases:
        with raises(AssertionError) as aerr:
            test_case_file = os.getenv(test_case, None)
            assert test_case_file is not None
            _generic_test__merge_many_records_group(test_case_file=test_case_file)


def _generic_test__merge_single_record_groups(test_case_file):
    test_case_json = load_ingest_test_case(test_case_file=test_case_file)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    records = test_case.get("records", None)
    assert records is not None
    records = _process_tested_records(records)

    model_name = test_case.get("model_name", None)
    assert model_name is not None

    records_one_record_groups_starts = test_case.get("records_one_record_groups_starts", None)
    assert records_one_record_groups_starts is not None

    p = RewardedDecisionPartition(model_name=model_name)
    records_not_nans_mask = _get_records_not_nan_mask(records)

    expected_output = test_case_json.get("expected_output", None)
    assert expected_output is not None
    expected_output = _process_expected_output(expected_output)

    merged_records = np.full(expected_output.shape, np.nan, dtype=object)
    # 'passthrough' records not in merged_records_one_record_groups_indices index

    merged_records_one_record_groups_indices = test_case.get("merged_records_one_record_groups_indices", None)
    assert merged_records_one_record_groups_indices is not None

    passthrough_indices = \
        list(set(np.arange(expected_output.shape[0])).difference(set(merged_records_one_record_groups_indices)))
    merged_records[passthrough_indices, :] = expected_output[passthrough_indices, :]

    # records, records_not_nans_mask, records_one_record_groups_starts,
    #             merged_records_one_record_groups_indices, merged_records

    p._merge_one_record_groups(
        records=records, records_not_nans_mask=records_not_nans_mask,
        records_one_record_groups_starts=np.array(records_one_record_groups_starts),
        merged_records_one_record_groups_indices=np.array(merged_records_one_record_groups_indices),
        merged_records=merged_records)

    # check if frames are equal
    merged_records_df = pd.DataFrame(merged_records, columns=DF_SCHEMA.keys()).astype(DF_SCHEMA)
    expected_output_df = pd.DataFrame(expected_output, columns=DF_SCHEMA.keys()).astype(DF_SCHEMA)

    pd.testing.assert_frame_equal(merged_records_df, expected_output_df)


def test__merge_one_record_groups_1():
    test_case_file = os.getenv('TEST__MERGE_SINGLE_RECORD_GROUPS_1_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_single_record_groups(test_case_file)
