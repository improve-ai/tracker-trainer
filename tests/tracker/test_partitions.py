import gzip
import numpy as np
import orjson
import os
from pytest import raises

import pandas as pd
from firehose_record import DF_SCHEMA, FirehoseRecord, FirehoseRecordGroup, \
    DECISION_ID_KEY, MESSAGE_ID_KEY, COUNT_KEY
from partition import RewardedDecisionPartition, parquet_s3_key_prefix
from tracker.tests_utils import load_ingest_test_case


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
        if jl.get(COUNT_KEY, 0) > 0:
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


def _prepare_tested_records(records):
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
    records = _prepare_tested_records(records)

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
        records=records, records_not_nans_mask=records_not_nan_mask,
        group_slice_start=group_slice_start, group_slice_end=group_slice_end,
        into=merged_records[0])
    expected_output = test_case_json.get('expected_output', None)
    assert expected_output is not None
    expected_output = _process_expected_output(expected_output)

    merged_df = \
        pd.DataFrame(merged_records, columns=DF_SCHEMA).astype(DF_SCHEMA)
    expected_df = \
        pd.DataFrame(expected_output, columns=DF_SCHEMA.keys()).astype(DF_SCHEMA)
    # TODO pandas assert frame equal passes but numpy wont allow
    #  np.nan == np.nan of dtype object to pass
    pd.testing.assert_frame_equal(merged_df, expected_df)


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
    records = _prepare_tested_records(records)

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

    merged_records_one_record_groups_indices = test_case.get("merged_records_one_record_groups_indices", None)
    assert merged_records_one_record_groups_indices is not None

    passthrough_indices = \
        list(set(np.arange(expected_output.shape[0])).difference(set(merged_records_one_record_groups_indices)))
    merged_records[passthrough_indices, :] = expected_output[passthrough_indices, :]

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
    # test case with has one 2 record group and other single records group
    # only 2 records group has any rewards
    test_case_file = os.getenv('TEST__MERGE_SINGLE_RECORD_GROUPS_1_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_single_record_groups(test_case_file)


def test__merge_one_record_groups_2():
    # only decision records, all single groups, no rewards
    test_case_file = os.getenv('TEST__MERGE_SINGLE_RECORD_GROUPS_2_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_single_record_groups(test_case_file)


def test__merge_one_record_groups_3():
    # only decision records, all single groups, last record has rewards and bad value of reward
    # this is to check if reward will be recalculated properly
    test_case_file = os.getenv('TEST__MERGE_SINGLE_RECORD_GROUPS_3_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_single_record_groups(test_case_file)


def test__merge_one_record_groups_4():
    # N - 1 decision records, 1 reward record with non- null "rewards" and "reward"
    test_case_file = os.getenv('TEST__MERGE_SINGLE_RECORD_GROUPS_4_JSON', None)
    assert test_case_file is not None
    _generic_test__merge_single_record_groups(test_case_file)


def _generic_test__get_group_slicing_indices(test_case_file):
    test_case_json = load_ingest_test_case(test_case_file=test_case_file)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    records = test_case.get("records", None)
    assert records is not None
    records = _prepare_tested_records(records)

    model_name = test_case.get("model_name", None)
    assert model_name is not None

    p = RewardedDecisionPartition(model_name=model_name)
    p.sorted = True

    groups_slices_starts, groups_slices_ends = \
        p._get_groups_slices_indices(records=records)

    expected_output = test_case_json.get("expected_output", None)
    assert expected_output is not None

    expected_groups_slices_starts = expected_output.get("groups_slices_starts", None)
    assert expected_groups_slices_starts is not None

    expected_groups_slices_ends = expected_output.get("groups_slices_ends", None)
    assert expected_groups_slices_ends is not None

    np.testing.assert_array_equal(expected_groups_slices_starts, groups_slices_starts)

    np.testing.assert_array_equal(expected_groups_slices_ends, groups_slices_ends)


def test__get_group_slicing_indices_1():
    test_case_file = os.getenv('TEST__GET_GROUPS_SLICES_INDICES_1_JSON', None)
    assert test_case_file is not None
    _generic_test__get_group_slicing_indices(test_case_file)


def test__get_group_slicing_indices_2():
    test_case_file = os.getenv('TEST__GET_GROUPS_SLICES_INDICES_2_JSON', None)
    assert test_case_file is not None
    _generic_test__get_group_slicing_indices(test_case_file)


def test__get_group_slicing_indices_3():
    test_case_file = os.getenv('TEST__GET_GROUPS_SLICES_INDICES_3_JSON', None)
    assert test_case_file is not None
    _generic_test__get_group_slicing_indices(test_case_file)


def test__get_group_slicing_indices_4():
    test_case_file = os.getenv('TEST__GET_GROUPS_SLICES_INDICES_4_JSON', None)
    assert test_case_file is not None
    _generic_test__get_group_slicing_indices(test_case_file)


def test__get_group_slicing_indices_raises_for_not_sorted_df():
    p = RewardedDecisionPartition(model_name='dummy-model')
    with raises(AssertionError) as aerr:
        p._get_groups_slices_indices(records=np.array([]))
