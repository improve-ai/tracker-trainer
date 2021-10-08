# STL imports
import string

# External imports
from pytest_cases import parametrize
from pytest_cases import parametrize_with_cases

# Local imports
from assign_rewards.history_record import MODEL_NAME_REGEXP
from assign_rewards.history_record import _is_valid_model_name


def test__is_valid_model_name():
    """
    Assert the passed parameter returns false for:
      - a non-str parameter
      - a zero-length str
      - a str not matching MODEL_NAME_REGEXP
    """

    assert _is_valid_model_name(1) == False
    assert _is_valid_model_name([]) == False
    assert _is_valid_model_name({}) == False
    assert _is_valid_model_name(None) == False
    assert _is_valid_model_name("") == False
    
    punctuation = list(string.punctuation)
    punctuation.remove(".")
    punctuation.remove("-")
    punctuation.remove("_")
    for i in punctuation:
        assert _is_valid_model_name(i) == False
        assert _is_valid_model_name(i+"a") == False

    for i in string.whitespace:
        assert _is_valid_model_name(i) == False
        
    assert _is_valid_model_name("a") == True
    assert _is_valid_model_name("aa") == True

