# Built-in imports
import os

# External imports
import pytest
import boto3
from moto import mock_s3
from pytest_cases import fixture

# Local imports
from firehose_record import MESSAGE_ID_KEY
from firehose_record import MODEL_KEY
from firehose_record import ITEM_KEY
from firehose_record import CONTEXT_KEY
from firehose_record import COUNT_KEY
from firehose_record import SAMPLE_KEY
from firehose_record import REWARD_KEY
from firehose_record import DECISION_ID_KEY
from firehose_record import FirehoseRecord


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
            MODEL_KEY      : model_val,            
        }

        if type_val == "decision":
            record[ITEM_KEY]    = variant_val
            record[CONTEXT_KEY]     = givens_val
            record[COUNT_KEY]      = count_val
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


def is_valid_givens(x):
    return (x is None) or isinstance(x, dict)


def is_valid_count(x):
    return isinstance(x, int) and (x >= 1)


def naive_is_valid_runners_up(x):
    """ Naive because there is also other validation on runners up that 
    depends on the number of variants, not part of this function """
    if x is None:
        return True
    elif isinstance(x, list) and len(x) > 0:
        return True

    return False


def is_valid_reward(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


def is_valid_decision_id(x):
    return is_valid_ksuid(x)


def is_valid_message_id(x):
    return is_valid_ksuid(x)


def is_valid_sample(x):
    try:
        orjson.dumps(x)
        return True
    except:
        return False


def assert_valid_record(json_dict):
    assert isinstance(json_dict, dict), "record is not a dict"
    
    ##########################################################################
    # Common fields assertions
    ##########################################################################
    message_id = json_dict[MESSAGE_ID_KEY]
    assert is_valid_message_id(message_id), f"invalid message_id: {message_id}"
    
    rec_type = json_dict[TYPE_KEY]
    assert is_valid_type(rec_type), f"invalid type: {rec_type}"

    model_name = json_dict[MODEL_KEY]
    assert is_valid_model_name(model_name), f"invalid model name: {model_name}"


    ##########################################################################
    # Decision record fields assertions
    ##########################################################################
    if json_dict[TYPE_KEY] == "decision":

        assert is_valid_givens(json_dict[CONTEXT_KEY]), "invalid 'givens'"
        
        runners_up = json_dict.get(RUNNERS_UP_KEY, None)
        assert naive_is_valid_runners_up(runners_up), "invalid' runners up'"

        count = json_dict[COUNT_KEY]
        assert is_valid_count(count), f"invalid 'count': {count}"

        has_sample = SAMPLE_KEY in json_dict
        sample_pool_size = _get_sample_pool_size(count, runners_up)

        assert sample_pool_size >= 0, "invalid 'count' or 'runners up'"

        if has_sample:
            assert sample_pool_size > 0, "invalid 'count' or 'runners up'"
        else:
            assert sample_pool_size == 0, "missing sample"
    

    ##########################################################################
    # Reward record fields assertions
    ##########################################################################
    elif json_dict[TYPE_KEY] == "reward":

        assert is_valid_decision_id(json_dict[DECISION_ID_KEY]), \
            "invalid 'decision id'"

        assert is_valid_reward(json_dict[REWARD_KEY]), "invalid 'reward'"


def assert_valid_rewarded_decision_record(rdr_dict, record_type):
    """
    """

    assert record_type in ("reward", "decision")

    ##########################################################################
    # decision_id validation
    ##########################################################################
    assert is_valid_decision_id(rdr_dict[DECISION_ID_KEY]), \
        f"invalid decision_id: {rdr_dict[DECISION_ID_KEY]}"


    ##########################################################################
    # rewards validation
    ##########################################################################
    rewards = rdr_dict.get(REWARDS_KEY)
    
    if rewards is not None:

        assert isinstance(rewards, str),  \
            f"{REWARDS_KEY} must be a json str, got {type(rewards)}"

        rewards_dict = orjson.loads(rewards)
        assert isinstance(rewards_dict, dict), \
            f"the json str 'rewards' must contain a dict, got {type(rewards_dict)}"
        
        for key,val in rewards_dict.items():

            assert is_valid_ksuid(key), \
                f"invalid message_id of one reward in 'rewards': {key}"

            assert is_valid_reward(val), \
                f"invalid reward in 'rewards'"
    

    ##########################################################################
    # reward validation
    ##########################################################################
    reward = rdr_dict.get(REWARD_KEY)
    if reward is not None:
        assert is_valid_reward(reward), f"invalid {REWARD_KEY}"
        
        assert REWARDS_KEY in rdr_dict, \
            f"{REWARD_KEY} present but {REWARDS_KEY} is missing"
        
        assert isinstance(rewards, str),  \
            f"{REWARDS_KEY} must be a json str, got {type(rewards)}"
        
        rewards_dict = orjson.loads(rewards)
        assert isinstance(rewards_dict, dict), \
            f"the json str 'rewards' must contain a dict, got {type(rewards_dict)}"
        
        for key in rewards_dict.keys():
            assert is_valid_decision_id(key), "invalid 'decision id' in 'rewards'"

        assert reward == sum(rewards_dict.values()), \
            f"{REWARD_KEY} != sum({REWARDS_KEY})"
    

    if record_type == "decision":

        ######################################################################
        # variant validation
        ######################################################################
        variant = rdr_dict[ITEM_KEY]
        
        assert isinstance(variant, str), \
            f"'variant' must be a json string, got {type(variant)}"
        
        assert orjson.loads(variant) is not None, \
            "'variant' must be non-null: {variant}"


        ######################################################################
        # givens validation
        ######################################################################
        givens = rdr_dict[CONTEXT_KEY]
        assert isinstance(givens, str), \
            f"'givens' must be a json str, got {type(givens)}"
        assert isinstance(orjson.loads(givens), dict), \
            f"the json str 'givens' must be a dict, got {type(orjson.loads(givens))}"


        ######################################################################
        # count validation
        ######################################################################
        count = rdr_dict[COUNT_KEY]
        assert is_valid_count(count), f"invalid 'count': {count}"


        ######################################################################
        # runners_up validation
        ######################################################################
        runners_up = rdr_dict.get(RUNNERS_UP_KEY)
        
        if runners_up is not None:
            
            assert isinstance(runners_up, str), "'runners_up' should be a str" 

            runners_up_list = orjson.loads(runners_up)
            assert isinstance(runners_up_list, list), \
                "'runners_up' must be a list"

            assert len(runners_up_list) > 0, "len(runners_up) must be > 0"
    
        # runners up must not be set if missing
        elif runners_up is None:
            assert RUNNERS_UP_KEY not in rdr_dict, \
                f"{RUNNERS_UP_KEY} must not be set if missing"


        ######################################################################
        # sample validation
        ######################################################################
        # sample must not be set if missing
        sample = rdr_dict.get(SAMPLE_KEY)
        if sample is None:
            assert SAMPLE_KEY not in rdr_dict, \
                f"{SAMPLE_KEY} must not be set if missing"
        
        else:
            assert isinstance(sample, str), f"'sample' must be a str: {sample}"
            assert is_valid_sample(orjson.loads(sample)), \
                f"invalid 'sample': {sample}"


    elif record_type == "reward":

        ######################################################################
        # variant validation
        ######################################################################
        variant = rdr_dict.get(ITEM_KEY)
        assert variant is None , \
            (f"in a partial rewarded decision record, "
             f"'variant' must be None: {variant}")

