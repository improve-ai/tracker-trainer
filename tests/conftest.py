# Built-in imports
import os

# External imports
import pytest
import boto3
from moto import mock_s3
from pytest_cases import fixture

# Local imports
from firehose_record import MESSAGE_ID_KEY
from firehose_record import TYPE_KEY
from firehose_record import MODEL_KEY
from firehose_record import VARIANT_KEY
from firehose_record import GIVENS_KEY
from firehose_record import COUNT_KEY
from firehose_record import RUNNERS_UP_KEY
from firehose_record import SAMPLE_KEY
from firehose_record import REWARD_KEY
from firehose_record import DECISION_ID_KEY
from firehose_record import FirehoseRecord
from firehose_record import assert_valid_record
from firehose_record import assert_valid_rewarded_decision_record


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
    def to_rewarded_decision_record(x):
        """ Transform a record into a rewarded decision record ready to
        be used in tests """
        return FirehoseRecord(x).to_rewarded_decision_dict()


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
        model_val  = "test-model-name-1.0",
        
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
def get_decision_rec(get_record):
    """ An fabric of decision records with some known values """
    
    def __dec_rec(msg_id_val="000000000000000000000000000"):
        return get_record(
            type_val = "decision",
            msg_id_val = msg_id_val)
    
    return __dec_rec


@fixture
def get_rewarded_decision_rec(get_decision_rec, helpers):

    def __rdr(decision_id='000000000000000000000000000'):
        decision_record = get_decision_rec(msg_id_val=decision_id)
        rdr = helpers.to_rewarded_decision_record(decision_record)
        assert_valid_rewarded_decision_record(rdr, record_type="decision")
        return rdr

    return __rdr


@fixture
def get_reward_rec(get_record):
    """ An instance of a reward record with some known values """
    
    # Automatic different message_ids
    idval = 990
        
    def __rew_rec(msg_id_val=None, decision_id_val='000000000000000000000000000', reward_val= -10):
        # Automatic different message_ids
        nonlocal idval
        if msg_id_val is None:
            msg_id_val = str(idval).rjust(27, "0")
        idval += 1

        record = get_record(
            type_val        = "reward",
            msg_id_val      = msg_id_val,
            decision_id_val = decision_id_val,
            reward_val      = reward_val)

        return record
    
    return __rew_rec


@fixture
def get_partial_rewarded_dec_rec(get_reward_rec, helpers):

    def __partial_rewarded_dec_rec(msg_id_val="000000000000000000000000001"):
        reward_record = get_reward_rec(msg_id_val=msg_id_val)
        prdr = helpers.to_rewarded_decision_record(reward_record)
        assert_valid_rewarded_decision_record(prdr, record_type="reward")
        return prdr
        
    return __partial_rewarded_dec_rec