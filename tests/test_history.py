# Built-in imports
import random
import string

# External imports
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize
import rstr

# Local imports
from history import is_valid_hashed_history_id, hash_history_id, history_dir_for_hashed_history_id
from history import HASHED_HISTORY_ID_REGEXP
from history import HASHED_HISTORY_ID_LEN
import config


HEX_CHARS = "0123456789abcdef"

class CasesHashedHistoryIds:
    def case_valid(self):
        return rstr.xeger(HASHED_HISTORY_ID_REGEXP)

    @parametrize("x", [None, -1, 0, 1, [], {}, set()])
    def case_invalid_types(self, x):
        return x
    
    @parametrize("x", ["", " ", "   ", "z32245015571b8e9ed3dc63e33d6366882413ace1032ff21a584082e2db0c521"])
    def case_invalid_strings(self, x):
        return x
    
    def case_invalid_too_long(self):
        hash_chars = [random.choice(HEX_CHARS) for h in range(HASHED_HISTORY_ID_LEN + 1)]
        return "".join(hash_chars)
    
    def case_invalid_too_short(self):
        hash_chars = [random.choice(HEX_CHARS) for h in range(HASHED_HISTORY_ID_LEN - 1)]
        return "".join(hash_chars)
    
    def case_invalid_pattern(self):
        hash_chars = [random.choice(string.printable) for h in range(HASHED_HISTORY_ID_LEN)]
        return  "".join(hash_chars)

    def case_valid(self):
        hash_chars = [random.choice(HEX_CHARS) for h in range(HASHED_HISTORY_ID_LEN)]
        return "".join(hash_chars)


@parametrize_with_cases("hashed_history_id", cases=CasesHashedHistoryIds)
def test_is_valid_hashed_history_id(hashed_history_id, current_cases):

    case_id, fun, params = current_cases["hashed_history_id"]

    if case_id.startswith("invalid"):
        assert is_valid_hashed_history_id(hashed_history_id) == False
    elif case_id.startswith("valid"):
        assert is_valid_hashed_history_id(hashed_history_id) == True


def test_hash_history_id():
    assert hash_history_id("a") == "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
    assert hash_history_id("b") == "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"


def test_history_dir_for_hashed_history_id():
    hashed_history_id = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
    result = history_dir_for_hashed_history_id(hashed_history_id)
    assert result == config.HISTORIES_PATH / "ca" / "97"