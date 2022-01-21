# Built-in imports
import string
import io

# External imports
import pytest
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize

# Local imports
import utils
from utils import find_first_gte
from utils import list_s3_keys_containing
from config import TRAIN_BUCKET
from firehose_record import DF_SCHEMA
from ingest_firehose_tests.utils import dicts_to_df


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
    #           ^                  ^
    #         start               end
    @parametrize("start,end,expected", [
        ("a", "e", ["b", "c", "d", "e"]),
        ("b", "d", ["c", "d"]),
        ("b", "a", "will raise exception"),
    ])
    def case_requested_keys_are_subset_of_available_keys1(self, start, end, expected):
        return string.ascii_lowercase

    # Keys: ----||||||||||||----------||||||||---------
    #                   ^        ^
    #                 start     end
    @parametrize("start,end,expected", [
        ("j", "m", ["k", "l", "o"]),
        ("m", "j", "will raise exception"),
    ])
    def case_requested_range_is_partially_in_hole1(self, start, end, expected):
        return ['i', 'j', 'k', 'l', 'o']


    # Keys: ----||||||||----------||||||||---------
    #                       ^        ^
    #                     start     end
    @parametrize("start,end,expected", [("m", "o", ["o"]),])
    def case_requested_range_is_partially_in_hole2(self, start, end, expected):
        return ['i', 'j', 'k', 'l', 'o', 'p']


    # Keys: ----|||||||||||||||||||||||---------
    #           ^                     ^
    #          start                  end
    @parametrize("start,end,expected", [
        ("a", "z", string.ascii_lowercase[1:]),
        ("z", "a", "will raise exception"),
    ])
    def case_requested_keys_are_the_actual_start_and_end(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #           ^                         ^
    #          start                     end
    @parametrize("start,end,expected", [
        ("a", "}", string.ascii_lowercase[1:]),
        ("}", "a", "will raise exception"),
    ])
    def case_end_key_is_beyond_available(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #         ^                       ^
    #        start                   end
    @parametrize("start,end,expected", [
        ("9", "z", string.ascii_lowercase),
        ("z", "9", "will raise exception"),
    ])
    def case_start_key_is_before_available(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #         ^                          ^
    #        start                      end
    @parametrize("start,end,expected", [
        ("9", "}", string.ascii_lowercase),
        ("}", "9", "will raise exception"),
    ])
    def case_requested_keys_are_before_and_after_available(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||------------
    #                                    ^       ^
    #                                  start    end
    @parametrize("start,end,expected", [
        ("{", "}", []),
        ("}", "{", "will raise exception"),
    ])
    def case_requested_keys_are_after_available1(self, start, end, expected):
        return string.ascii_lowercase

    # Keys: ----||||||||||---------
    #                      ^   ^
    #                   start  end
    @parametrize("start,end,expected", [
        ("a", "z", []),
        ("0", "9", ["1", "2", "3", "4"]),
    ])
    def case_requested_keys_are_after_available2(self, start, end, expected):
        return ["0", "1", "2", "3", "4"]

    # Keys: -----------|||||||||||||||||||||||-------
    #        ^       ^
    #   start/end  end/start
    @parametrize("start,end,expected", [
        ("0", "9", ["a"]),
        ("9", "0", "will raise exception"),
    ])
    def case_requested_keys_are_before_available(self, start, end, expected):
        return string.ascii_lowercase


    @parametrize("start,end", [
        (None, "}"),
        ("}", None),
        (None, None),
        ([], "a"),
        ("a", []),
        ({}, "a"),
        ("a", {}),
        (1, "a"),
        ("a", 1),
        (1.1, 1.2),
    ])
    def case_wrong_types(self, start, end):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #                  ^
    #             start == end
    @parametrize("start,end,expected", [
        ("c", "c", ["d"]),
    ])
    def case_requested_keys_are_the_same1(self, start, end, expected):
        return string.ascii_lowercase
    

    # Keys: ----|||||||||||||||||||||||---------
    #        ^
    #   start == end
    @parametrize("start,end,expected", [
        ("0", "0", ["a"]),
    ])
    def case_requested_keys_are_the_same2(self, start, end, expected):
        return string.ascii_lowercase
    
    
    # Keys: ----|||||||||||||||||||||||---------
    #                                     ^
    #                               start == end
    @parametrize("start,end,expected", [
        ("}", "}", []),
    ])
    def case_requested_keys_are_the_same3(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||-------|||||||||---------
    #                      ^
    #                 start == end
    @parametrize("start,end,expected", [
        ("m", "m", ["o"]),
    ])
    def case_requested_keys_are_the_same4(self, start, end, expected):
        return ['i', 'j', 'k', 'l', 'o', 'p']


    @parametrize("start,end,expected", [
        ("aa", "c", ["aac", "abcd", "b", "z"]),
    ])
    def case_simple_longer_keys(self, start, end, expected):
        return ['a', 'aa', 'b', 'aac', 'z', 'abcd']


    @parametrize("start,end,expected", [("a", "b", [])])
    def case_no_available_keys(self, start, end, expected):
        return []


@parametrize_with_cases("keys", cases=CasesS3Keys)
def test_list_s3_keys_containing(s3, current_cases, mocker, keys):

    # Retrieve cases information
    case_id, fun, params = current_cases["keys"]

    # Retrieve start key, end key and expected results
    p_start  = params.get("start")
    p_end    = params.get("end")
    expected = params.get("expected")

    # Test for known exceptions to be raised
    if case_id == "wrong_types":
        with pytest.raises(TypeError):
            list_s3_keys_containing(TRAIN_BUCKET, p_start, p_end)
    
    # Test for known exceptions to be raised
    elif p_start > p_end:
        with pytest.raises(ValueError):
            list_s3_keys_containing(TRAIN_BUCKET, p_start, p_end)
    
    else:
        # TODO this can be simplified by moving with statement here
        #  this way we will be aware where s3 comes from and how it is created
        # Patch the s3 client used in list_delimited_s3_keys
        utils.s3client = s3
        # Create a temporal bucket
        s3.create_bucket(Bucket=TRAIN_BUCKET)

        # Put the case keys in the bucket
        for s3_key in keys:
            s3.put_object(Bucket=TRAIN_BUCKET, Body=io.BytesIO(), Key=s3_key)

        selected_keys = list_s3_keys_containing(TRAIN_BUCKET, p_start, p_end, valid_keys_only=False)

        assert isinstance(selected_keys, list)
        assert len(selected_keys) == len(expected)
        assert all([a == b for a, b in zip(selected_keys, expected)])


class CasesFindToTheRight:


    # Keys: |||||||||||||||||||||||---------
    #                                    ^
    #                                   mark
    @parametrize("mark,expected", [("{", None)])
    def case_requested_key_beyond_available(self, mark, expected):
        return string.ascii_lowercase


    # Keys: |||||||||||||||||||||||---------
    #                             ^
    #                            mark
    @parametrize("mark,expected", [("z", "z")])
    def case_requested_key_is_last_available(self, mark, expected):
        return string.ascii_lowercase
    

    # Keys: --------|||||||||||||||||||||||---------
    #               ^
    #              mark
    @parametrize("mark,expected", [("a", "a")])
    def case_requested_key_is_first_available(self, mark, expected):
        return string.ascii_lowercase


    # Keys: --------|||||||||||||||||||||||---------
    #            ^
    #           mark
    @parametrize("mark,expected", [("A", "a")])
    def case_requested_key_is_before_available(self, mark, expected):
        return string.ascii_lowercase


    # Keys: ----------------------------------
    #                ^
    #               mark
    @parametrize("mark,expected", [("whatever", None)])
    def case_no_keys_available(self, mark, expected):
        return []


    # Keys: --------|||||-------||||---------------
    #                       ^
    #                      mark
    @parametrize("mark,expected", [("m", "o")])
    def case_requested_key_is_in_hole(self, mark, expected):
        return ['i', 'j', 'k', 'l', 'o']


    # Keys: --------|||||-------||||---------------
    #                   ^
    #                  mark
    @parametrize("mark,expected", [("l", "l")])
    def case_requested_key_is_just_before_hole(self, mark, expected):
        return ['i', 'j', 'k', 'l', 'o']

    # Keys: --------|||||-------||||---------------
    #                           ^
    #                          mark
    @parametrize("mark,expected", [("o", "o")])
    def case_requested_key_is_just_after_hole(self, mark, expected):
        return ['i', 'j', 'k', 'l', 'o']


@parametrize_with_cases("keys", cases=CasesFindToTheRight)
def test_find_first_gte(keys, current_cases):

    case_id, fun, params = current_cases["keys"]
    mark, expected = params["mark"], params["expected"]
    
    i,v = find_first_gte(mark, keys)
    assert v == expected


def test_incorrectly_named_s3_key(s3, tmp_path, get_rewarded_decision_rec):
    """
    Assert that keys that don't comply with:
        src.ingest_firehose.utils.REWARDED_DECISIONS_S3_KEY_REGEXP
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
    s3_keys = list_s3_keys_containing(
        bucket_name     = TRAIN_BUCKET,
        start_after_key = f'rewarded_decisions/{model_name}/parquet/',
        end_key         = f'rewarded_decisions/{model_name}/parquet/ZZZZ',
        prefix          = f'rewarded_decisions/{model_name}')

    assert s3_key not in s3_keys