import json

from config import PARQUET_FILE_MAX_DECISION_RECORDS
from partition import list_partition_s3_keys, min_max_timestamp_row_count
from utils import is_valid_model_name

def filter_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')

    model_name = event['model_name']
    assert is_valid_model_name(model_name)

    # list every partition for this model_name
    s3_keys = list_partition_s3_keys(model_name)

    return filter_partitions(s3_keys)
    
    
def filter_partitions(s3_keys):
    print(f'filtering {len(s3_keys)} partitions')
    groups = []
    group = []

    # reverse() to aggregate partitions from newest to oldest
    for s3_key in s3_keys.reverse():
        group.append(s3_key)        
        
        merge_group = []
        min_timestamp, max_timestamp, row_count = min_max_timestamp_row_count(s3_key)
        
    
    return groups if len(groups) else None
    

def groom_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')
