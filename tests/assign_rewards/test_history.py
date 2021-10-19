# Built-in imports
import random
import string
from pathlib import Path
import os
from uuid import uuid4

# External imports
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize
import rstr
from uuid import uuid4

# Local imports
from history import is_valid_hashed_history_id
from history import hash_history_id
from history import history_dir_for_hashed_history_id
from history import hashed_history_id_from_file
from history import select_incoming_history_files_for_node
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


def test_hashed_history_id_from_file():
    file_path = Path("7a8b238803f3e42727996a9825a7514b4e81cd94534ddb58ecd6e13c41f93b42-0c3fa530-302c-45b3-b93a-c84a89208dcd.jsonl.gz")
    hashed_history_id = hashed_history_id_from_file(file_path)
    assert hashed_history_id == "7a8b238803f3e42727996a9825a7514b4e81cd94534ddb58ecd6e13c41f93b42"


def test_select_incoming_history_files_for_node(monkeypatch):
    """
    Test a function which returns correctly named files:
        select_incoming_history_files_for_node()
    
    To do so, use synthetic data where there is a known number of 
    correctly and incorrectly named files.

    Note:
    You can use the following function to generate the synthetic data,
    although it should already be commited: generate_known_files()
    """

    # If you want to change this, you gotta re-generate the data
    NUMBER_OF_NODES_TO_SIMULATE = 3

    # Monkeypatch an specific number of nodes
    monkeypatch.setattr(config, "NODE_COUNT", NUMBER_OF_NODES_TO_SIMULATE)

    # Simulate being a specific node
    for node_id in range(NUMBER_OF_NODES_TO_SIMULATE):

        # Reset count of bad files
        monkeypatch.setattr(config.stats, "bad_file_name_count", 0)

        # Patch NODE_ID to simulate being such node
        monkeypatch.setattr(config, "NODE_ID", node_id)

        # Patch INCOMING_PATH to make it point to test_data
        test_data_path = Path(os.environ['TEST_DATA_DIR']) / "assign_rewards" / f"node_count_{NUMBER_OF_NODES_TO_SIMULATE}_node_id_{node_id}"
        monkeypatch.setattr(config, "INCOMING_PATH", test_data_path)

        # The actual assertion of number of known good and bad filenames
        selected_files = select_incoming_history_files_for_node()
        assert config.stats.bad_file_name_count == node_id
        assert len(selected_files) == NUMBER_OF_NODES_TO_SIMULATE


def generate_known_files():
    """
    Generate a folder structure with json.gz files named like: 
        {hashed_history_id}-{uuid4}.jsonl.gz 
    
    The folders will be named like:
        - node_count_3_node_id_0
        - node_count_3_node_id_1
        - node_count_3_node_id_2

    Inside each folder there will be a known number of files whose name match:
        int(filename[:8], 16) % node_count) == node_id

    and a known number of files (`node_id` files) whose name doesn't 
    match the above condition. These known files will be used to assert
    a correct behavior of the function select_incoming_history_files_for_node()

    Example:

    ├─ tests\data\assign_rewards
    │  └─ node_count_3_node_id_0
    │     ├─ 1644a842720fc3250e3332f8490d8b44182c8eb3322464ac3d995938c80b8829-fd45c71d-d290-4be2-a3fe-fd5bc6778925.jsonl.gz
    │     └─ ...
    │  └─ node_count_3_node_id_1
    │     ├─ b684b0f3477a3732481f7e51da9799bedeea1684682ce97e9fd3eed85c5c20e9-fcbd5e26-b415-4543-aae7-5634d5b5ca1f.jsonl.gz
    │     └─ ...
    │  └─ node_count_3_node_id_2
    │     ├─ 139db9ac9cc604b248d70fb08db08cc7eb447db15aeda5c879ff05f5a3370fb1-70044248-b281-4e9a-af01-33f1da957fa7.jsonl.gz
    │     └─ ...
    """

    for node_id in range(3):

        # Path towards dir with synthetic test data
        data_path = Path(os.environ['TEST_DATA_DIR']) / "assign_rewards"

        # Make subdir where files will be found that match:
        #     int(hex_name, 16) % node_count) == node_id
        subdir_name = f"node_count_3_node_id_{node_id}"
        (data_path/subdir_name).mkdir(exist_ok=True)
        
        # Generate n hash-like strings
        hex_names_for_node_id = get_hex_names(n=3, node_count=3, node_id=node_id)
        
        # Create correctly named .jsonl.gz dummy files
        for hex_name in hex_names_for_node_id:
            
            # Concatenate an expected actual path towards the synthetic file
            history_filename = f"{hex_name}-{str(uuid4())}.jsonl.gz"
            file_path = data_path / subdir_name / history_filename

            # Generate dummy file
            with open(file_path, 'wb') as gzf: gzf.write(b"dummy")

        # Generate `node_id` wrongly named jsonl.gz files
        for i in range(node_id):
            file_path = data_path / subdir_name / f"wronggly_named-{str(uuid4())}.jsonl.gz"
            with open(file_path, 'wb') as gzf: gzf.write(b"dummy")


def gen():
    """Generate a random string made of hex chars of fixed length"""
    return "".join([random.choice(HEX_CHARS) for i in range(HASHED_HISTORY_ID_LEN)])


def get_hex_names(n, node_count, node_id):
    """Generate n hashes whose first 8 characters' decimal version % `node_count` equals `node_id` """
    hex_names = []
    while len(hex_names) < n:
        hex_name = gen()
        if int(hex_name[:8], 16) % node_count == node_id:
            hex_names.append(hex_name)
    return hex_names


def test_hash_history_id():
    assert hash_history_id("a") == "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
    assert hash_history_id("b") == "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"


def test_history_dir_for_hashed_history_id():
    hashed_history_id = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
    result = history_dir_for_hashed_history_id(hashed_history_id)
    assert result == config.HISTORIES_PATH / "ca" / "97"