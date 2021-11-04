# Built-in imports
import os

# External imports
import pytest
import boto3
from moto import mock_s3
from pytest_cases import fixture

# Local imports
from firehose_record import MESSAGE_ID_KEY, TIMESTAMP_KEY, TYPE_KEY
from firehose_record import MODEL_KEY
from firehose_record import VARIANT_KEY, GIVENS_KEY, COUNT_KEY, RUNNERS_UP_KEY, SAMPLE_KEY


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

@fixture
def get_record():
    """ A factory fixture.
    Advantages: https://stackoverflow.com/a/51663667/1253729
    """
    
    def __get_record(
        # common fields
        msg_id_val = "000000000000000000000000000",
        ts_val     = "2021-10-07T07:24:06.126+02:00",
        type_val   = None,
        model_val  = "messages-2.0",
        
        # type == "decision" fields
        count_val      = 3, # depends on runners_up length
        variant_val    = { "text": "Have the courage to surrender to what happens next." },
        givens_val     = { "device" : "iPhone", "page" : 2462, "shared" : { "a": 1 }, },
        runners_up_val = [ { "text": "You are safe." } ],
        sample_val     = { "text": "sample text" },

        # type == reward fields
        decision_id_val = "777777777777777777777777777",
        reward_val      = 1
        ):
        """Return a default valid record """

        # Assert is valid ksuid
        # TODO: add more validations
        assert isinstance(msg_id_val, str)
        assert len(msg_id_val) == 27

        # Assert valid timestamp
        assert isinstance(ts_val, str)

        # Assert type
        assert isinstance(type_val, str)
        assert type_val in ("decision", "reward")

        # Assert model name
        # TODO: actually assert something here

        if type_val == "decision":
            # TODO: variant assertions

            # Assert givens
            assert (givens_val is None) or isinstance(givens_val, dict)

            # Assert count
            assert isinstance(count_val, int)
            assert count_val >= 1

            # Assert runners__up. Can be None, otherwise a non-empty list
            assert (runners_up_val is None) or isinstance(runners_up_val, list)
            if isinstance(runners_up_val, list):
                assert len(runners_up_val) > 0

            # Assert sample
            # TODO: assertions on sample
        
        elif type_val == "reward":
            
            # Assert decision_id
            assert isinstance(decision_id_val, str)
            assert len(decision_id_val) == 27

            # Assert reward
            # TODO: add more assertions
            assert isinstance(reward_val, int) or isinstance(reward_val, float)

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
            record["decision_id"] = decision_id_val
            record["reward"] = reward_val

        return record
    
    return __get_record