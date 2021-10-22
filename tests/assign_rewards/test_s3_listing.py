# Built-in imports
import string
import io

# External imports
import pytest
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize

# Local imports
from assign_rewards.utils import find_first_gte
from assign_rewards.utils import list_s3_keys_containing

S3_BUCKET = "temp"

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

class CasesS3Keys:

    # Keys: ----|||||||||||||||||||||||---------
    #             ^             ^
    #           start          end
    @parametrize("start,end,expected", [
        ("a", "b", ["a", "b"]),
        ("b", "d", ["b", "c", "d"]),
        ("b", "a", "will raise exception"),
    ])
    def case_requested_keys_are_subset_of_available_keys1(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #                   ^        ^
    #                 start     end
    @parametrize("start,end,expected", [
        ("k", "m", ["k", "l", "m"]),
        ("m", "k", "will raise exception"),
    ])
    def caserequested_keys_are_subset_of_available_keys2(self, start, end, expected):
        return ['i', 'j', 'k', 'l', 'm', 'n', 'o']


    # Keys: ----||||||||||||----------||||||||---------
    #                   ^        ^
    #                 start     end
    @parametrize("start,end,expected", [
        ("j", "m", ["j", "k", "l", "o"]),
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
        ("a", "z", string.ascii_lowercase),
        ("z", "a", "will raise exception"),
    ])
    def case_requested_keys_are_the_actual_start_and_end(self, start, end, expected):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #           ^                         ^
    #          start                     end
    @parametrize("start,end,expected", [
        ("a", "}", string.ascii_lowercase),
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
    def case_requested_keys_are_after_available(self, start, end, expected):
        return string.ascii_lowercase

    # Keys: -----------|||||||||||||||||||||||-------
    #        ^       ^
    #   start/end  end/start
    @parametrize("start,end,expected", [
        ("0", "9", ["a"]),
        ("9", "0", "will raise exception"),
    ])
    def case_requested_keys_are_after_available(self, start, end, expected):
        return string.ascii_lowercase


    @parametrize("start,end", [
        (None, "}"),
        ("}", None),
        (None, None),
        (None, None),
        (1, 1),
        (1, 1),
    ])
    def case_wrong_types(self, start, end):
        return string.ascii_lowercase


    # Keys: ----|||||||||||||||||||||||---------
    #                  ^
    #             start == end
    @parametrize("start,end,expected", [
        ("c", "c", ["c"]),
    ])
    def case_requested_keys_are_the_same(self, start, end, expected):
        return string.ascii_lowercase
    

    @parametrize("start,end,expected", [
        ("aa", "c", ["aa", "aac", "b", "z"]),
    ])
    def case_simple_longer_keys(self, start, end, expected):
        return ['a', 'aa', 'b', 'aac', 'z']


    @parametrize("start,end,expected", [
        ("a", "z", []),
        ("0", "9", ["0", "1", "2", "3", "4"]),
    ])
    def case_custom_4(self, start, end, expected):
        return ["0", "1", "2", "3", "4"]


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

    # Patch the s3 client used in list_delimited_s3_keys
    mocker.patch('config.s3client', new=s3)
    
    # Test for known exceptions to be raised
    if case_id == "wrong_types":
        with pytest.raises(TypeError):
            selected_keys = list_s3_keys_containing(S3_BUCKET, p_start, p_end)
    
    # Test for known exceptions to be raised
    elif p_start > p_end:
        with pytest.raises(ValueError):
            selected_keys = list_s3_keys_containing(S3_BUCKET, p_start, p_end)
    
    else:
        # Create a temporal bucket
        s3.create_bucket(Bucket=S3_BUCKET)

        # Put the case keys in the bucket
        for s3_key in keys:
            s3.put_object(Bucket=S3_BUCKET, Body=io.BytesIO(), Key=s3_key)

        selected_keys = list_s3_keys_containing(S3_BUCKET, p_start, p_end)

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