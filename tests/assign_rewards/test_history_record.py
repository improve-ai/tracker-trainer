# Built-in imports
from pathlib import Path
from datetime import datetime
import string

# External imports
import pytest
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize
from dateutil.parser import parse
from dateutil import tz
import rstr

# Local imports
from src.train.constants import MODEL_NAME_REGEXP
from history_record import HistoryRecord
from history_record import _is_valid_model_name
from history_record import _load_records
from history_record import MESSAGE_ID_KEY, TIMESTAMP_KEY, TYPE_KEY
from history_record import MODEL_KEY
from history_record import REWARD_KEY, VARIANT_KEY, GIVENS_KEY, COUNT_KEY, RUNNERS_UP_KEY, SAMPLE_KEY
from history_record import PROPERTIES_KEY, VALUE_KEY
import config


class CasesRecords:

    def case_valid_record(self, get_record):
        return get_record()
    
    def case_missing_variant(self, get_record):
        r = get_record()
        del r[VARIANT_KEY]
        return r
    
    # A missing timestamp raises an error and that's tested elsewhere
    # def case_missing_timestamp(self):
    #     pass


@parametrize_with_cases("r", cases=CasesRecords)
def test_to_rewarded_decision_dict(r, current_cases):
    """
    Test a decision record's generated dict representation is correct.

    Parameters
    ----------
    r: pytest-cases' a case parameter
    current_cases: pytest-cases fixture
    """

    # Access the case details
    case_id, fun, params = current_cases["r"]
    
    record = HistoryRecord(r)
    result = record.to_rewarded_decision_dict()
    
    if case_id == "missing_variant":
        assert result[VARIANT_KEY] is None

    elif case_id == "valid_record":        
        assert isinstance(result, dict)

        # Assert all these keys are present in the result
        FIXED_KEYS = [TIMESTAMP_KEY, REWARD_KEY, VARIANT_KEY]
        OTHER_KEYS = [GIVENS_KEY, COUNT_KEY, RUNNERS_UP_KEY, SAMPLE_KEY]
        for key in FIXED_KEYS+OTHER_KEYS: assert key in result

        # Assert types
        assert isinstance(result[TIMESTAMP_KEY], str)
        assert isinstance(result[REWARD_KEY], int) or isinstance(result[REWARD_KEY], float)
        assert isinstance(result[GIVENS_KEY], dict)
        # assert isinstance(result[VARIANT_KEY], whatever) # All variant types are valid
        assert isinstance(result[COUNT_KEY], int) 
        assert isinstance(result[RUNNERS_UP_KEY], list) 
        # assert isinstance(result[SAMPLE_KEY], whatever) # All sample types are valid

        # Assert these values are in the dict
        assert result[TIMESTAMP_KEY]  == "2021-10-07T07:24:06.126000+02:00"
        assert result[REWARD_KEY]     == 0
        assert result[VARIANT_KEY]    == r[VARIANT_KEY]
        assert result[GIVENS_KEY]     == r[GIVENS_KEY]
        assert result[COUNT_KEY]      == r[COUNT_KEY]
        assert result[RUNNERS_UP_KEY] == r[RUNNERS_UP_KEY]
        assert result[SAMPLE_KEY]     == r[SAMPLE_KEY]


class CasesTimestamps:

    record_base = {
        TIMESTAMP_KEY   : "2021-10-07T07:00:00.126+0{h}:00",
    }

    def case_in_window_same_tz(self):
        ts = self.record_base[TIMESTAMP_KEY].format(h=0)
        return {
            TIMESTAMP_KEY : (parse(ts) + config.REWARD_WINDOW/2).isoformat(),
        }

    def case_in_window_limit_same_tz(self):
        ts = self.record_base[TIMESTAMP_KEY].format(h=0)
        return {
            TIMESTAMP_KEY : (parse(ts) + config.REWARD_WINDOW).isoformat(),
        }

    def case_in_window_different_tz(self):
        ts = self.record_base[TIMESTAMP_KEY].format(h=5)
        return {
            TIMESTAMP_KEY : (parse(ts) + config.REWARD_WINDOW/2).isoformat(),
        }

    def case_out_of_window_same_tz(self):
        ts = self.record_base[TIMESTAMP_KEY].format(h=0)
        return {
            TIMESTAMP_KEY : (parse(ts) + config.REWARD_WINDOW*1.1).isoformat(),
        }

    def case_out_of_window_different_tz(self):
        ts = self.record_base[TIMESTAMP_KEY].format(h=5)
        return {
            TIMESTAMP_KEY : (parse(ts) + config.REWARD_WINDOW*1.1).isoformat(),
        }


@parametrize_with_cases("r", cases=CasesTimestamps)
def test_reward_window_contains(r, current_cases):
    """
    Test that a timestamp falls or not within a time window with 
    respect to another timestamp.

    Parameters
    ----------
    r: pytest-cases' a case parameter
    current_cases: pytest-cases fixture
    """
    record_base = CasesTimestamps.record_base
    record_base[TIMESTAMP_KEY] = record_base[TIMESTAMP_KEY].format(h=0)
    record1 = HistoryRecord(record_base)
    record2 = HistoryRecord(r)

    # Access the case details
    case_id, fun, params = current_cases["r"]

    if case_id == "in_window_same_tz":
        assert record1.reward_window_contains(record2) == True

    if case_id == "case_in_window_limit_same_tz":
        assert record1.reward_window_contains(record2) == True
    
    elif case_id == "in_window_different_tz":
        assert record1.reward_window_contains(record2) == True

    elif case_id == "out_of_window_same_tz":
        assert record1.reward_window_contains(record2) == False
    
    elif case_id == "out_of_window_different_tz":
        assert record1.reward_window_contains(record2) == False


def test_record_type(get_record):
    """ Test is_event_record and is_decision_record for different types"""

    r = get_record(type_val="decision")
    record = HistoryRecord(r)
    assert record.is_event_record() == False
    assert record.is_decision_record() == True
    
    r = get_record(type_val="event")
    record = HistoryRecord(r)
    assert record.is_event_record() == True
    assert record.is_decision_record() == False

    
class CasesValidInvalidRecords:
    """

    Case invalid_empty: empty dict
    Case invalid_msg: invalid message_id
    Case invalid_ts : invalid timestamps
    Cases invalid_missing_message: missing message_id key
    Cases invalid_missing_ts: missing timestamp key
    Cases invalid_missing_type: missing type key
    Cases invalid_model: missing model key
    Cases invalid_missing_count: missing count key
    Cases valid: all valid cases
    """

    def case_invalid_empty(self):
        return {}
    
    @parametrize("msg_val", [None])
    def case_invalid_msg(self, get_record, msg_val):
        return get_record(msg_val=msg_val)

    @parametrize("ts_val", [None, "2021-99-99T07:24:06.126+02:00"])
    def case_invalid_timestamp(self, get_record, ts_val):
        return get_record(ts_val=ts_val)

    @parametrize("type_val", [None])
    def case_invalid_type(self, get_record, type_val):
        return get_record(type_val=type_val)

    @parametrize("model_val", ["", None])
    def case_invalid_model(self, get_record, model_val):
        return get_record(type_val="decision", model_val=model_val)

    @parametrize("count_val", ["", None, -1.1, -1, 0, 0.0, 0.1, 1.1, "1", "0"])
    def case_invalid_count(self, get_record, count_val):
        return get_record(type_val="decision", count_val=count_val)

    def case_invalid_missing_timestamp(self, get_record):
        r = get_record()
        del r[TIMESTAMP_KEY]
        return r

    def case_invalid_missing_message(self, get_record):
        r = get_record()
        del r[MESSAGE_ID_KEY]
        return r

    def case_invalid_missing_type(self, get_record):
        r = get_record()
        del r[TYPE_KEY]
        return r

    def case_invalid_missing_count(self, get_record):
        r = get_record(type_val="decision")
        del r[COUNT_KEY]
        return r

    def case_invalid_missing_type(self, get_record):
        r = get_record(type_val="decision")
        del r[MODEL_KEY]
        return r

    @parametrize("type_val", ["decision", "event"])
    def case_valid(self, get_record, type_val):
        return get_record(type_val=type_val)
    
    @parametrize("ts_val", [
        '2021-01-27T00:00:00.000000-0500',
        '2021-01-27T00:00:00.000001',
        '2021-01-27T00:00:00'
    ])
    def case_valid_timestamps(self, get_record, ts_val):
        return get_record(ts_val=ts_val)


@parametrize_with_cases("r", cases=CasesValidInvalidRecords)
def test_is_valid(r, current_cases):
    """Test the validity of valid/invalid records """
    
    # Access the case details
    case_id, fun, params = current_cases["r"]

    # Handle invalid cases
    if case_id.startswith("invalid"):

        if TIMESTAMP_KEY not in r:
            pytest.xfail("Implementation will change")
            # with pytest.raises(TypeError):
            #     record = HistoryRecord(r)
        
        elif case_id == "invalid_timestamp" and r.get(TIMESTAMP_KEY) is None:
            pytest.xfail("Implementation will change")
            # with pytest.raises(TypeError):
            #     record = HistoryRecord(r)

        # The code for these cases of invalid timestamps will change
        # and this handling of an attribute error is temporary
        elif case_id == "invalid_timestamp" and r.get(TIMESTAMP_KEY) is not None:
            pytest.xfail("Implementation will change")
            # record = HistoryRecord(r)
            # assert record.is_valid() == False
        
        else:
            record = HistoryRecord(r)
            assert record.is_valid() == False

    # Handle valid cases
    elif case_id.startswith("valid"):
        if case_id == "valid_timestamps":
            record = HistoryRecord(r)
            if params["ts_val"] == "2021-01-27T00:00:00.000000-0500":
                tzinfo = tz.tzstr('GMT-04:00')
                diff = record.timestamp - datetime(2021, 1, 27, 0, 0, 0, tzinfo=tzinfo)
                assert diff.total_seconds() == 3600
            elif params["ts_val"] == "2021-01-27T00:00:00.000001":
                diff = record.timestamp - datetime(2021, 1, 27, 0, 0, 0)
                assert diff.total_seconds() == 1e-6
            elif params["ts_val"] == "2021-01-27T00:00:00":
                diff = record.timestamp - datetime(2021, 1, 27, 0, 0, 0)
                assert diff.total_seconds() == 0
        else:
            record = HistoryRecord(r)
            assert record.is_valid() == True


def test_history_record(get_record):
    """Test other internal properties of HistoryRecord"""
    
    r = get_record(type_val="event")
    
    record = HistoryRecord(r)
    assert isinstance(record.value, int) or isinstance(record.value, float)
    assert record.value == config.DEFAULT_EVENT_VALUE

    r[PROPERTIES_KEY] = {VALUE_KEY : 1}
    record = HistoryRecord(r)
    assert isinstance(record.value, int) or isinstance(record.value, float)
    assert record.value == 1


def test__is_valid_model_name():
    """ """

    # Assert that non-str and empty strings are not valid
    for i in [-1, 0, 1, [], {}, set(), None, "", " "]:
        assert _is_valid_model_name(i) == False
    
    # Assert that punctuation signs are not accepted
    chars = list(string.punctuation)
    chars.remove(".")
    chars.remove("-")
    chars.remove("_")

    for i in chars:
        assert _is_valid_model_name(i) == False
        assert _is_valid_model_name(f"{i}a") == False
        assert _is_valid_model_name(f"a{i}") == False
        assert _is_valid_model_name(f"a{i}a") == False
    
    # Assert some simple valid model name examples are valid
    for i in ["a", "aa", "a-a", "a_a", "a.a", "a-", "a_", "a."]:
        assert _is_valid_model_name(i) == True

    # Assert some real model name examples are valid
    for i in ["messages-2.0", "songs-2.0", "stories-2.0", "appconfig"]:
        assert _is_valid_model_name(i) == True

    # Assert some randomly generated model names are valid 
    for i in range(1000):
        random_str = rstr.xeger(MODEL_NAME_REGEXP)
        assert _is_valid_model_name(random_str) == True


def test__load_records():
    """
    Missing:
    - If a bad Gzip is passed, make sure a file in /unrecoverable is written out.
    - config.stats is incremented by one
    """
    
    synthetic_data = Path("./tests/test_data/dummy_incoming/incoming").glob("*.jsonl.gz")

    for f in synthetic_data:
        records = _load_records(f, set())
        assert isinstance(records, list)
        for r in records:
            assert isinstance(r, HistoryRecord)


