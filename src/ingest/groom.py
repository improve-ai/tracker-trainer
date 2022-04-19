import json

from config import PARQUET_FILE_MAX_DECISION_RECORDS, MAX_GROOM_ITERATIONS
from partition import RewardedDecisionPartition, list_partition_s3_keys, min_timestamp, max_timestamp, row_count
from utils import is_valid_model_name


def filter_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')

    model_name = event['model_name']
    assert is_valid_model_name(model_name)
    
    try:
        iteration = int(event['filter']['iteration'])+1
    except (KeyError, ValueError):
        iteration = 1
        
    if iteration > MAX_GROOM_ITERATIONS:
        return None
    
    # list every partition for this model_name
    s3_keys = list_partition_s3_keys(model_name)

    print(f'filtering {len(s3_keys)} partitions')

    groups = group_partitions_to_groom(s3_keys)
    
    return {'iteration': iteration, 'groom_groups': list(groups)} # wrap in list for JSON serializiation
    
    
def group_partitions_to_groom(s3_keys):
    
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

    s3_keys = event['s3_keys']

    print(f'grooming {len(s3_keys)} rewarded decision partitions containing {sum(map(row_count, s3_keys))} records')
    
    # load all s3_keys, merge records, and optionally split into multiple partitions
    RewardedDecisionPartition(model_name=event['model_name'], s3_keys=s3_keys).process()
    
    return None