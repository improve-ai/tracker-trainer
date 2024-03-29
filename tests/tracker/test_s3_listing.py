# Built-in imports
import io
import os
from pathlib import Path
import string
import tempfile

# External imports
import numpy as np
import pandas as pd
import pytest
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize

# Local imports
import src.ingest.config

src.ingest.config.FIREHOSE_BUCKET = os.getenv('FIREHOSE_BUCKET', None)
assert src.ingest.config.FIREHOSE_BUCKET is not None

src.ingest.config.TRAIN_BUCKET = os.getenv('TRAIN_BUCKET', None)
assert src.ingest.config.TRAIN_BUCKET is not None

import config
import partition
import firehose_record
import utils
from utils import list_s3_keys
from partition import list_partition_s3_keys as list_partitions
from config import TRAIN_BUCKET
from firehose_record import DF_SCHEMA
from tracker.tests_utils import dicts_to_df, get_valid_s3_key_from_df, load_ingest_test_case, \
    get_model_name_from_env, are_all_s3_keys_valid, _prepare_s3_for_list_partition_tests


ENGINE = "fastparquet"

"""
Some characters (from string.printable) ordered by their Unicode code point:

for c in sorted(string.printable): print(f"{c}: {ord(c)}")

    !: 33
    ...
    /: 47
    0: 48
    ...
    9: 57
    ...
    =: 61
    ...
    A: 65
    ...
    Z: 90
    ...
    _: 95
    ...
    a: 97
    ...
    z: 122
    {: 123
    |: 124
    }: 125
    ~: 126
"""

"""
TODO: add case which uses the prefix
TODO: start by testing the high level behavior:


Equal to or after a specific decision_id, which is equivalent to after a prefix of the decision_id

bucket: [rewarded_decisions/messages-2.0/parquet/2021/11/03/20QAxKOXA-20QAxNXYi-20QAxMZns2RdfX64tNXdPVf4OSY.parquet]
start_after_key: rewarded_decisions/messages-2.0/parquet/2021/11/03/20QAxKOXA
end_key:

result: rewarded_decisions/messages-2.0/parquet/2021/11/03/20QAxKOXA-20QAxNXYi-20QAxMZns2RdfX64tNXdPVf4OSY.parquet

"""


class CasesS3Keys:

    # Keys: ----|||||||||||||||||||||||---------
    #           ^
    #         start
    @parametrize("start,expected", [
        ("a", string.ascii_lowercase[1:]),
        ("b", string.ascii_lowercase[2:]),
    ])
    def case_requested_keys_are_subset_of_available_keys1(self, start, expected):
        return string.ascii_lowercase

    # Keys: ----||||||||||||----------||||||||---------
    #                   ^
    #                 start
    @parametrize("start,expected", [
        ("j", ["k", "l", "o"]),
        ("m", ["o"]),
    ])
    def case_requested_range_is_partially_in_hole1(self, start, expected):
        return ['i', 'j', 'k', 'l', 'o']

    # Keys: ----||||||||----------||||||||---------
    #                       ^
    #                     start
    @parametrize("start,expected", [("m", ["o", "p"]),])
    def case_requested_range_is_partially_in_hole2(self, start, expected):
        return ['i', 'j', 'k', 'l', 'o', 'p']

    # Keys: ----|||||||||||||||||||||||---------
    #           ^
    #          start
    @parametrize("start,expected", [
        ("a", string.ascii_lowercase[1:]),
        ("z", []),
    ])
    def case_requested_keys_are_the_actual_start_and_end(self, start, expected):
        return string.ascii_lowercase

    @parametrize("start,expected", [
        ("}", []),
        ("9", string.ascii_lowercase)
    ])
    def case_digits_and_special_cahracters(self, start, expected):
        return string.ascii_lowercase

    # Keys: ----|||||||||||||||||||||||------------
    #                                    ^
    #                                  start
    @parametrize("start,expected", [
        ("{", []),
        ("}", []),
    ])
    def case_requested_keys_are_after_available1(self, start, expected):
        return string.ascii_lowercase

    # Keys: ----||||||||||---------
    #                      ^
    #                   start
    @parametrize("start,expected", [
        ("a", []),
        ("0", ["1", "2", "3", "4"]),
    ])
    def case_requested_keys_are_after_available2(self, start, expected):
        return ["0", "1", "2", "3", "4"]

    # Keys: -----------|||||||||||||||||||||||-------
    #        ^       ^
    #   start/end  end/start
    @parametrize("start,expected", [
        ("0", string.ascii_lowercase),
        ("9", string.ascii_lowercase),
    ])
    def case_requested_keys_are_before_available(self, start, expected):
        return string.ascii_lowercase

    @parametrize("start", [
        (None, ),
        (None, ),
        ([], ),
        ({}, ),
        (1, ),
        (1.1, ),
    ])
    def case_wrong_types(self, start):
        return string.ascii_lowercase

    @parametrize("start,expected", [
        ("aa", ["aac", "abcd", "b", "z"]),
    ])
    def case_simple_longer_keys(self, start, expected):
        return ['a', 'aa', 'b', 'aac', 'z', 'abcd']

    @parametrize("start,expected", [("a", [])])
    def case_no_available_keys(self, start, expected):
        return []


@parametrize_with_cases("keys", cases=CasesS3Keys)
def test_list_s3_keys_after(s3, current_cases, mocker, keys):

    # Retrieve cases information
    case_id, fun, params = current_cases["keys"]

    # Retrieve start key, end key and expected results
    p_start  = params.get("start")
    expected = params.get("expected")
    # Patch the s3 client used in list_delimited_s3_keys
    config.s3client = utils.s3client = \
        partition.s3client = firehose_record.s3client = s3
    # Create a temporal bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # Test for known exceptions to be raised
    if case_id == "wrong_types":
        with pytest.raises(TypeError):
            list_s3_keys(TRAIN_BUCKET, after_key=p_start)

    else:
        # TODO this can be simplified by moving with statement here
        #  this way we will be aware where s3 comes from and how it is created
        # Put the case keys in the bucket
        for s3_key in keys:
            s3.put_object(Bucket=TRAIN_BUCKET, Body=io.BytesIO(), Key=s3_key)

        selected_keys = list_s3_keys(TRAIN_BUCKET, after_key=p_start)

        assert isinstance(selected_keys, map)
        assert len(list(selected_keys)) == len(expected)
        assert all([a == b for a, b in zip(selected_keys, expected)])


@parametrize_with_cases("keys", cases=CasesS3Keys)
def test_list_partitions_after(s3, current_cases, mocker, keys):

    # Retrieve cases information
    case_id, fun, params = current_cases["keys"]

    # Retrieve start key, end key and expected results
    p_start  = params.get("start")
    expected = params.get("expected")
    # Patch the s3 client used in list_delimited_s3_keys
    config.s3client = utils.s3client = \
        partition.s3client = firehose_record.s3client = s3
    # Create a temporal bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # Test for known exceptions to be raised
    if case_id == "wrong_types":
        with pytest.raises(TypeError):
            list_partitions(TRAIN_BUCKET, after_key=p_start)

    else:
        # TODO this can be simplified by moving with statement here
        #  this way we will be aware where s3 comes from and how it is created
        # Put the case keys in the bucket
        for s3_key in keys:
            s3.put_object(Bucket=TRAIN_BUCKET, Body=io.BytesIO(), Key=s3_key)

        selected_keys = list_s3_keys(TRAIN_BUCKET, after_key=p_start)

        assert isinstance(selected_keys, map)
        assert len(list(selected_keys)) == len(expected)
        assert all([a == b for a, b in zip(selected_keys, expected)])


def test_incorrectly_named_s3_partition(s3, get_rewarded_decision_rec):
    """
    Assert that keys that don't comply with: constants.REWARDED_DECISIONS_S3_KEY_REGEXP
    won't be returned.
    
    Parameters:
        s3 : pytest fixture
        tmp_path : pytest fixture
        get_rewarded_decision_rec : pytest fixture
    """
    
    # Create mock bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # Create a dummy parquet file
    invalid_filename = 'file.parquet'
    model_name = get_model_name_from_env()

    rdr = get_rewarded_decision_rec()
    df = dicts_to_df(dicts=[rdr], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

    invalid_key_prefix = "/".join(get_valid_s3_key_from_df(df=df, model_name=model_name).split("/")[:-2])

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        # valid_key_prefix = os.sep.join(valid_s3_key.split(os.sep)[:-1])
        invalid_partition_dir = tmp_path / invalid_key_prefix
        # make sure parent dir exists
        invalid_partition_dir.mkdir(parents=True, exist_ok=True)
        invalid_s3_key_path = invalid_partition_dir / invalid_filename

        df.to_parquet(invalid_s3_key_path, engine=ENGINE, index=False)

        # Upload file with a key that doesn't comply with the expected format
        invalid_s3_key = invalid_key_prefix + '/' + invalid_filename
        with invalid_s3_key_path.open(mode='rb') as f:
            s3.upload_fileobj(
                Fileobj   = f,
                Bucket    = TRAIN_BUCKET,
                Key       = invalid_s3_key,
                ExtraArgs = { 'ContentType': 'application/gzip' })

        # Ensure the key is really there
        response = s3.list_objects_v2(
            Bucket = TRAIN_BUCKET,
            Prefix = f'rewarded_decisions/{model_name}')
        all_keys = [x['Key'] for x in response['Contents']]
        assert invalid_s3_key in all_keys

        # Ensure the key is not listed by the function of interest
        s3_keys = list(list_partitions(model_name=model_name))
        if len(s3_keys) > 0:
            assert are_all_s3_keys_valid(s3_keys)

        assert invalid_s3_key not in list(s3_keys)


def test_incorrectly_named_s3_partition_in_correct_folder(s3, get_rewarded_decision_rec):
    """
    Assert that keys that don't comply with:
        constants.REWARDED_DECISIONS_S3_KEY_REGEXP
    won't not be returned.

    Parameters:
        s3 : pytest fixture
        tmp_path : pytest fixture
        get_rewarded_decision_rec : pytest fixture
    """

    # Create mock bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # Create a dummy parquet file
    invalid_filename = 'file.parquet'
    model_name = get_model_name_from_env()

    rdr = get_rewarded_decision_rec()
    df = dicts_to_df(dicts=[rdr], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

    valid_key_prefix = "/".join(get_valid_s3_key_from_df(df=df, model_name=model_name).split("/")[:-1])

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        valid_partition_dir = tmp_path / valid_key_prefix
        # make sure parent dir exists
        valid_partition_dir.mkdir(parents=True, exist_ok=True)
        invalid_s3_key_path = valid_partition_dir / invalid_filename

        df.to_parquet(invalid_s3_key_path, engine=ENGINE, index=False)

        # Upload file with a key that doesn't comply with the expected format
        invalid_s3_key = valid_key_prefix + "/" + invalid_filename
        with invalid_s3_key_path.open(mode='rb') as f:
            s3.upload_fileobj(
                Fileobj=f,
                Bucket=TRAIN_BUCKET,
                Key=invalid_s3_key,
                ExtraArgs={'ContentType': 'application/gzip'})

        # Ensure the key is really there
        response = s3.list_objects_v2(
            Bucket=TRAIN_BUCKET,
            Prefix=f'rewarded_decisions/{model_name}')
        all_keys = [x['Key'] for x in response['Contents']]
        assert invalid_s3_key in all_keys

        # Ensure the key is not listed by the function of interest
        s3_keys = list(list_partitions(model_name=model_name))
        if len(list(s3_keys)) > 0:
            assert are_all_s3_keys_valid(s3_keys)

        assert invalid_s3_key not in list(s3_keys)


def test_correctly_named_s3_partition(s3, get_rewarded_decision_rec):
    """
    Assert that keys that don't comply with:
        constants.REWARDED_DECISIONS_S3_KEY_REGEXP
    won't be returned.

    Parameters
    ----------
    s3: boto3.client
        pytest fixture
    get_rewarded_decision_rec: callable
        pytest fixture
    """

    # Create mock bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # TODO verify that those fnames are valid keys
    filenames = [
        '20220129T185234Z-20220129T184832Z-152-6f678cec-74d0-4a87-be0d-00f811fcc391.parquet',
        '20220129T185434Z-20220129T185237Z-78-94013f07-f824-45fc-b398-24b940991e71.parquet']

    model_name = get_model_name_from_env()

    for filename in filenames:

        rdr = get_rewarded_decision_rec()
        df = dicts_to_df(dicts=[rdr], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
        valid_key_prefix = "/".join(get_valid_s3_key_from_df(df=df, model_name=model_name).split("/")[:-1])

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            valid_partition_dir = tmp_path / valid_key_prefix
            # make sure parent dir exists
            valid_partition_dir.mkdir(parents=True, exist_ok=True)
            valid_s3_key_path = valid_partition_dir / filename

            # Create a dummy parquet file
            df.to_parquet(valid_s3_key_path, engine=ENGINE, index=False)

            # model_name = 'messages-2.0'
            valid_s3_key = valid_key_prefix + "/" + filename
            assert utils.is_valid_rewarded_decisions_s3_key(valid_s3_key)

            # Upload file with a key that doesn't comply with the expected format
            with valid_s3_key_path.open(mode='rb') as f:
                s3.upload_fileobj(
                    Fileobj=f,
                    Bucket=TRAIN_BUCKET,
                    Key=valid_s3_key,
                    ExtraArgs={'ContentType': 'application/gzip'})

    # Ensure the key is really there
    response = s3.list_objects_v2(
        Bucket=TRAIN_BUCKET,
        Prefix=f'rewarded_decisions/{model_name}')
    all_keys = [x['Key'] for x in response['Contents']]

    # Ensure the key is not listed by the function of interest
    # list_partition_s3_keys(model_name)
    s3_keys = list(list_partitions(model_name=model_name))
    if len(list(s3_keys)) > 0:
        print(f's3_keys: {s3_keys}')
        assert are_all_s3_keys_valid(s3_keys)

    np.testing.assert_array_equal(all_keys, s3_keys)


# def _prepare_s3_for_list_partition_tests(s3_client, dfs, s3_keys):
#     # create train bucket
#     s3_client.create_bucket(Bucket=src.ingest.config.TRAIN_BUCKET)
#
#     if len(dfs) == 0 or len(s3_keys) == 0:
#         return
#
#     for df, s3_key in zip(dfs, s3_keys):
#         # use tempdir
#         with tempfile.TemporaryDirectory() as tmp_dir:
#             tmp_path = Path(tmp_dir)
#             # ensure dir exists
#             (tmp_path / "/".join(s3_key.split("/")[:-1])).mkdir(exist_ok=True, parents=True)
#             s3_key_path = tmp_path / s3_key
#             # cache file to disk before push to moto
#             df.to_parquet(s3_key_path, engine=ENGINE, index=False)
#
#             # assert utils.is_valid_rewarded_decisions_s3_key(s3_key)
#
#             # Upload file with a key that doesn't comply with the expected format
#             with s3_key_path.open(mode='rb') as f:
#                 s3_client.upload_fileobj(
#                     Fileobj=f,
#                     Bucket=TRAIN_BUCKET,
#                     Key=s3_key,
#                     ExtraArgs={'ContentType': 'application/gzip'})


# tests for `list_partition_s3_keys`
#  1. test against an empty bucket
def test_list_partition_s3_keys_empty_bucket(s3):
    dfs = []
    s3_keys = []
    # s3_client, dfs, s3_keys, bucket, engine
    _prepare_s3_for_list_partition_tests(
        s3, dfs, s3_keys, src.ingest.config.TRAIN_BUCKET, ENGINE)
    # _prepare_s3_for_list_partition_tests(s3, dfs, s3_keys)
    calculated = list_partitions(get_model_name_from_env())
    assert list(calculated) == []


def _generic_test_list_partition(test_case_file, s3_client):
    test_case_json = load_ingest_test_case(test_case_file=test_case_file)
    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    # get parquet files paths
    parquet_files = test_case.get("parquet_files", None)
    assert parquet_files is not None

    # load desired dfs
    parquet_files_dir = Path(os.getenv("TEST_CASES_DIR")) / os.getenv("MERGE_TEST_DATA_RELATIVE_DIR")
    dfs = [pd.read_parquet(parquet_files_dir / parquet_file) for parquet_file in parquet_files]

    # get target s3 keys
    s3_keys = test_case.get("s3_keys", None)
    assert s3_keys is not None

    # prepare moto bucket
    # _prepare_s3_for_list_partition_tests(s3_client, dfs, s3_keys)
    _prepare_s3_for_list_partition_tests(
        s3_client, dfs, s3_keys, src.ingest.config.TRAIN_BUCKET, ENGINE)

    # call list partition

    model_name = test_case.get("model_name", None)
    assert model_name is not None
    calculated = list(list_partitions(model_name))

    expected = test_case_json.get("expected_s3_keys", None)
    assert expected is not None

    np.testing.assert_array_equal(sorted(calculated), sorted(expected))


#  2. test against all valid keys
def test_list_partition_s3_keys_all_valid_keys(s3):
    _generic_test_list_partition(os.getenv("LIST_PARTITION_S3_KEYS_ALL_VALID_KEYS_JSON"), s3)


#  3. test against some valid and invalid keys
def test_list_partition_s3_keys_valid_and_invalid_keys(s3):
    _generic_test_list_partition(os.getenv("LIST_PARTITION_S3_KEYS_VALID_AND_INVALID_KEYS_JSON"), s3)


#  4. test against all invalid keys
def test_list_partition_s3_keys_all_invalid_keys(s3):
    _generic_test_list_partition(os.getenv("LIST_PARTITION_S3_KEYS_ALL_INVALID_KEYS_JSON"), s3)
