# Built-in imports
import string
import io

# External imports
import numpy as np
import pytest
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize

# Local imports
import config
import rewarded_decisions
import firehose_record
import utils
from utils import list_s3_keys_after, list_partitions_after
from config import TRAIN_BUCKET
from firehose_record import DF_SCHEMA
from tests_utils import dicts_to_df


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
        rewarded_decisions.s3client = firehose_record.s3client = s3
    # Create a temporal bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # Test for known exceptions to be raised
    if case_id == "wrong_types":
        with pytest.raises(TypeError):
            list_s3_keys_after(TRAIN_BUCKET, p_start)

    else:
        # TODO this can be simplified by moving with statement here
        #  this way we will be aware where s3 comes from and how it is created
        # Put the case keys in the bucket
        for s3_key in keys:
            s3.put_object(Bucket=TRAIN_BUCKET, Body=io.BytesIO(), Key=s3_key)

        selected_keys = list_s3_keys_after(TRAIN_BUCKET, p_start)

        assert isinstance(selected_keys, list)
        assert len(selected_keys) == len(expected)
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
        rewarded_decisions.s3client = firehose_record.s3client = s3
    # Create a temporal bucket
    s3.create_bucket(Bucket=TRAIN_BUCKET)

    # Test for known exceptions to be raised
    if case_id == "wrong_types":
        with pytest.raises(TypeError):
            list_partitions_after(TRAIN_BUCKET, p_start)

    else:
        # TODO this can be simplified by moving with statement here
        #  this way we will be aware where s3 comes from and how it is created
        # Put the case keys in the bucket
        for s3_key in keys:
            s3.put_object(Bucket=TRAIN_BUCKET, Body=io.BytesIO(), Key=s3_key)

        selected_keys = list_partitions_after(TRAIN_BUCKET, p_start, valid_keys_only=False)

        assert isinstance(selected_keys, list)
        assert len(selected_keys) == len(expected)
        assert all([a == b for a, b in zip(selected_keys, expected)])


def test_incorrectly_named_s3_partition(s3, tmp_path, get_rewarded_decision_rec):
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
    filename = 'file.parquet'
    parquet_filepath = tmp_path / filename
    rdr = get_rewarded_decision_rec()
    df = dicts_to_df(dicts=[rdr], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    df.to_parquet(parquet_filepath, engine=ENGINE, index=False)

    model_name = 'messages-2.0'
    s3_key = f'rewarded_decisions/{model_name}/parquet/{filename}'

    # Upload file with a key that doesn't comply with the expected format
    with parquet_filepath.open(mode='rb') as f:
        s3.upload_fileobj(
            Fileobj   = f,
            Bucket    = TRAIN_BUCKET,
            Key       = s3_key,
            ExtraArgs = { 'ContentType': 'application/gzip' })


    # Ensure the key is really there
    response = s3.list_objects_v2(
        Bucket = TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{model_name}')
    all_keys = [x['Key'] for x in response['Contents']]
    assert s3_key in all_keys


    # Ensure the key is not listed by the function of interest
    s3_keys = list_partitions_after(
        bucket_name= TRAIN_BUCKET,
        key=f'rewarded_decisions/{model_name}/parquet/',
        prefix=f'rewarded_decisions/{model_name}/')

    assert s3_key not in s3_keys


def test_incorrectly_named_s3_partition_in_correct_folder(s3, tmp_path, get_rewarded_decision_rec):
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
    filename = 'file.parquet'
    parquet_filepath = tmp_path / filename
    rdr = get_rewarded_decision_rec()
    df = dicts_to_df(dicts=[rdr], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    df.to_parquet(parquet_filepath, engine=ENGINE, index=False)

    model_name = 'messages-2.0'
    s3_key = f'rewarded_decisions/{model_name}/parquet/2022/01/29/{filename}'

    # Upload file with a key that doesn't comply with the expected format
    with parquet_filepath.open(mode='rb') as f:
        s3.upload_fileobj(
            Fileobj=f,
            Bucket=TRAIN_BUCKET,
            Key=s3_key,
            ExtraArgs={'ContentType': 'application/gzip'})

    # Ensure the key is really there
    response = s3.list_objects_v2(
        Bucket=TRAIN_BUCKET,
        Prefix=f'rewarded_decisions/{model_name}')
    all_keys = [x['Key'] for x in response['Contents']]
    assert s3_key in all_keys

    # Ensure the key is not listed by the function of interest
    s3_keys = list_partitions_after(
        bucket_name=TRAIN_BUCKET,
        key=f'rewarded_decisions/{model_name}/parquet/',
        prefix=f'rewarded_decisions/{model_name}/')

    assert s3_key not in s3_keys


def test_correctly_named_s3_partition(s3, tmp_path, get_rewarded_decision_rec):
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

    filenames = [
        '20220129T185234Z-20220129T184832Z-152-6f678cec-74d0-4a87-be0d-00f811fcc391.parquet',
        '20220129T185434Z-20220129T185237Z-78-94013f07-f824-45fc-b398-24b940991e71.parquet']

    for filename in filenames:
        # Create a dummy parquet file
        parquet_filepath = tmp_path / filename
        rdr = get_rewarded_decision_rec()
        df = dicts_to_df(dicts=[rdr], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
        df.to_parquet(parquet_filepath, engine=ENGINE, index=False)

        model_name = 'messages-2.0'
        s3_key = f'rewarded_decisions/{model_name}/parquet/2022/01/29/{filename}'

        # Upload file with a key that doesn't comply with the expected format
        with parquet_filepath.open(mode='rb') as f:
            s3.upload_fileobj(
                Fileobj=f,
                Bucket=TRAIN_BUCKET,
                Key=s3_key,
                ExtraArgs={'ContentType': 'application/gzip'})

    # Ensure the key is really there
    response = s3.list_objects_v2(
        Bucket=TRAIN_BUCKET,
        Prefix=f'rewarded_decisions/{model_name}')
    all_keys = [x['Key'] for x in response['Contents']]

    # Ensure the key is not listed by the function of interest
    s3_keys = list_partitions_after(
        bucket_name=TRAIN_BUCKET,
        key=f'rewarded_decisions/{model_name}/parquet/',
        prefix=f'rewarded_decisions/{model_name}/')

    np.testing.assert_array_equal(sorted(all_keys), s3_keys)
