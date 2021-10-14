# Built-in imports
import string

# Local imports
from utils import _is_valid_model_name


def test__is_valid_model_name():
    """
    
    messages-2.0, songs-2.0, stories-2.0, appconfig
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
