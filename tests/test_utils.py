# Built-in imports
import json
import random

# Local imports
from src.utils import load_records
from src.utils import sort_records_by_timestamp


def test_sort_records_by_timestamp(all_type_records):
    random.shuffle(all_type_records)
    
    sort_records_by_timestamp(all_type_records)

    assert all_type_records[0]['message_id'] == "decision_record_1"
    assert all_type_records[1]['message_id'] == "event_record_1"
    assert all_type_records[2]['message_id'] == "reward_record_1"
    assert all_type_records[3]['message_id'] == "decision_record_2"
    assert all_type_records[4]['message_id'] == "event_record_2"
    assert all_type_records[5]['message_id'] == "reward_record_2"
    assert all_type_records[6]['message_id'] == "decision_record_3"
    assert all_type_records[7]['message_id'] == "reward_record_3"
    assert all_type_records[8]['message_id'] == "event_record_3"


def test_load_records(decision_records, reward_records, event_records):
    """
    Create a fake jsonl file with unordered records, load and sort it and
    verify the correct order.
    """

    records = []
    records.extend(decision_records)
    records.extend(reward_records)
    records.extend(event_records)
    
    filename = "records.jsonl"

    with open(filename, "wt") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    
    (expected_decision_records, 
    expected_reward_records, 
    expected_event_records) = load_records(filename)

    assert decision_records == expected_decision_records
    assert reward_records == expected_reward_records
    assert event_records == expected_event_records
    