import json

from config import PARQUET_FILE_MAX_DECISION_RECORDS
from partition import list_partition_s3_keys, min_timestamp, max_timestamp, row_count
from utils import is_valid_model_name

def filter_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')

    model_name = event['model_name']
    assert is_valid_model_name(model_name)

    # list every partition for this model_name
    s3_keys = list_partition_s3_keys(model_name)

    groups = group_partitions_to_groom(s3_keys)
    
    return map(lambda x: {'groom_group': x}, groups)
    
    
def group_partitions_to_groom(s3_keys):
    print(f'filtering {len(s3_keys)} partitions')
    
    groups = group_small_adjacent_partitions(s3_keys)

    groups = merge_overlapping_adjacent_group_pairs(groups)
    
    # filter out single s3_keys that don't need to be merged
    groups = filter(lambda x: len(x) > 1, groups)
    
    return groups
    
    
def group_small_adjacent_partitions(s3_keys, max_row_count=PARQUET_FILE_MAX_DECISION_RECORDS):

    groups = []

    for s3_key in s3_keys:
        if len(groups) >= 1 and sum(map(row_count, groups[-1])) + row_count(s3_key) <= max_row_count:
            groups[-1].append(s3_key) # append to the previous group
        else:
            groups.append([s3_key]) # create a new group

    return groups
    
    
def merge_overlapping_adjacent_group_pairs(groups):
    
    result = []
    candidate_group = None

    for group in groups:
        assert len(group) >= 1
        
        if candidate_group and max(map(max_timestamp, candidate_group)) > min(map(min_timestamp, group)):
            candidate_group.extend(group)
            candidate_group = None # only merge pairs, not unbounded continuous runs of groups
        else:
            result.append(group)
            candidate_group = group

    return result

def groom_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')

    