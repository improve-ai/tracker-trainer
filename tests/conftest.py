# Built-in imports
import os

# External imports
import pytest
import boto3
from moto import mock_s3
from pytest_cases import fixture

# Local imports
from firehose_record import MESSAGE_ID_KEY
from firehose_record import TIMESTAMP_KEY
from firehose_record import TYPE_KEY
from firehose_record import MODEL_KEY
from firehose_record import VARIANT_KEY
from firehose_record import GIVENS_KEY
from firehose_record import COUNT_KEY
from firehose_record import RUNNERS_UP_KEY
from firehose_record import SAMPLE_KEY
from firehose_record import FirehoseRecord
from firehose_record import REWARD_KEY
from firehose_record import REWARDS_KEY
from firehose_record import assert_valid_record
from firehose_record import assert_valid_rewarded_decision_record
from firehose_record import DECISION_ID_KEY
from utils import utc
from utils import get_valid_timestamp
from utils import json_dumps_wrapping_primitive


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


class Helpers:
    """
    A collection of helper functions used when generating test data.
    """
    
    @staticmethod
    def fix_rewarded_decision_dict(d):
        """ To be able to pass a dict into a Pandas DataFrame """

        if "rewards" in d and isinstance(d["rewards"], dict):
            d["rewards"] = [d["rewards"]]


    @staticmethod
    def get_expected_rewarded_record(
        base_record,
        type_base_record,
        decision_id,
        rewards=None,
        reward=None):
        """
        Return a rewarded decision record based on the value of a decision 
        record but with some key values manually specified.
        """

        r = { "decision_id"  : decision_id }

        if type_base_record == "decision":
        
            if TIMESTAMP_KEY in base_record:
                timestamp = get_valid_timestamp(base_record[TIMESTAMP_KEY])
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=utc)
                
                r[TIMESTAMP_KEY] = timestamp

            r[VARIANT_KEY] = json_dumps_wrapping_primitive(base_record.get(VARIANT_KEY))
            r[GIVENS_KEY] = json_dumps_wrapping_primitive(base_record.get(GIVENS_KEY))
        
        else:
            # in a reward record and timesamp shouldn't be copied
            r[VARIANT_KEY] = "null"


        if COUNT_KEY in base_record:
            r[COUNT_KEY] = base_record[COUNT_KEY]
        
        if RUNNERS_UP_KEY in base_record:
            r[RUNNERS_UP_KEY] = [json_dumps_wrapping_primitive(x) for x in base_record[RUNNERS_UP_KEY]]
        
        if SAMPLE_KEY in base_record:
            r[SAMPLE_KEY] = json_dumps_wrapping_primitive(base_record[SAMPLE_KEY])
        
        if rewards is not None:
            r[REWARDS_KEY] = [rewards] # List-wrapped for pandas only
        
        if reward is not None:
            r[REWARD_KEY] = reward

        assert_valid_rewarded_decision_record(r, record_type=type_base_record)
        
        return r


    @staticmethod
    def to_rewarded_decision_record(x):
        rewarded_decision_rec = FirehoseRecord(x).to_rewarded_decision_dict()
        Helpers.fix_rewarded_decision_dict(rewarded_decision_rec)
        return rewarded_decision_rec


@fixture
def helpers():
    """ Return a class containing useful helper functions """
    return Helpers


@fixture
def get_record():
    """ A factory fixture.
    Advantages: https://stackoverflow.com/a/51663667/1253729
    """
    
    def __get_record(
        # Common fields
        msg_id_val = "000000000000000000000000000",
        ts_val     = "2021-10-07T07:24:06.126+02:00",
        type_val   = None,
        model_val  = "messages-2.0",
        
        # type == "decision" fields
        count_val      = 3, # depends on runners_up length
        variant_val    = { "text": "variant text" },
        givens_val     = { "device" : "iPhone", "page" : 2462, "shared" : { "a": 1 }, },
        runners_up_val = [ { "text": "You are safe." } ],
        sample_val     = { "text": "sample text" },

        # type == "reward" fields
        decision_id_val = "000000000000000000000000007",
        reward_val      = 1 ):
        """Return a default valid record """

        assert type_val in ("decision", "reward")

        # Record with common fields
        record = {
            MESSAGE_ID_KEY : msg_id_val,
            TIMESTAMP_KEY  : ts_val,
            TYPE_KEY       : type_val,
            MODEL_KEY      : model_val,            
        }

        if type_val == "decision":
            record[VARIANT_KEY]    = variant_val
            record[GIVENS_KEY]     = givens_val
            record[COUNT_KEY]      = count_val
            record[RUNNERS_UP_KEY] = runners_up_val
            record[SAMPLE_KEY]     = sample_val

        elif type_val == "reward":
            record[DECISION_ID_KEY] = decision_id_val
            record[REWARD_KEY] = reward_val

        assert_valid_record(record)

        return record
    

    return __get_record


@fixture
def dec_rec(get_record):
    """ An fabric of decision records with some known values """
    
    def __dec_rec(msg_id_val="000000000000000000000000000"):
        return get_record(
            type_val = "decision",
            msg_id_val = msg_id_val)
    
    return __dec_rec


@fixture
def rewarded_dec_rec(dec_rec, helpers):
    return helpers.to_rewarded_decision_record(dec_rec())


@fixture
def get_rew_rec(get_record):
    """ An instance of a reward record with some known values """
    
    def __rew_rec(msg_id_val="000000000000000000000000001"):
        record = get_record(
            type_val        = "reward",
            msg_id_val      = msg_id_val,
            decision_id_val = "000000000000000000000000000",
            reward_val      = -10)

        return record
    
    return __rew_rec


@fixture
def get_partial_rewarded_dec_rec(get_rew_rec, helpers):

    def __partial_rewarded_dec_rec(msg_id_val="000000000000000000000000001"):
        reward_record = get_rew_rec(msg_id_val=msg_id_val)
        partial_rewarded_dec_rec = helpers.to_rewarded_decision_record(reward_record)
        return partial_rewarded_dec_rec
        
    return __partial_rewarded_dec_rec