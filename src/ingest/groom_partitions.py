import json

from config import PARQUET_FILE_MAX_DECISION_RECORDS
from partition import list_partition_s3_keys
from utils import is_valid_model_name

def filter_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')

    model_name = event['model_name']
    assert is_valid_model_name(model_name)

    partition_s3_keys = list_partition_s3_keys(model_name)

    results = []
    for s3_key in partition_s3_keys:
        merge_group = []
        group_row_count = 0
        min_timestamp, max_timestamp, decision_record_count = min_max_timestamp_decision_record_count(s3_key)
    
    return results if len(results)

def groom_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')
