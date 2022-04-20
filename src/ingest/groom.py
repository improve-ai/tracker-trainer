import orjson

from config import PARQUET_FILE_MAX_DECISION_RECORDS, MAX_GROOM_ITERATIONS
from partition import RewardedDecisionPartition, list_partition_s3_keys, min_timestamp, max_timestamp, row_count
from utils import is_valid_model_name, json_dumps


def filter_handler(event, context):
    
    print(f'processing event {json_dumps(event)}')

    model_name = event['model_name']
    assert is_valid_model_name(model_name)
    
    try:
        iteration = int(event['filter']['iteration'])+1
    except (KeyError, ValueError):
        iteration = 1
        
    if iteration > MAX_GROOM_ITERATIONS:
        return None
    
    # list up to all partitions for this model_name
    s3_keys = list_partition_s3_keys(model_name)

    groups = group_partitions_to_groom(s3_keys)
    
    return {'iteration': iteration, 'groom_groups': list(groups)} # wrap in list for JSON serializiation
    
    
def group_partitions_to_groom(s3_keys):
    
    groups = group_small_adjacent_partitions(s3_keys)

    groups = merge_overlapping_adjacent_group_pairs(groups)
    
    # filter out single s3_keys that don't need to be merged
    groups = filter(lambda x: len(x) > 1, groups)
    
    # the maximum step function payload is 256KB so cap the yielded key bytes
    return cap_groups(groups)
    

def cap_groups(groups, max_s3_key_bytes=204800):
    s3_key_bytes = 0
    for group in groups:
        
        capped_group = []
        for s3_key in group:
            s3_key_bytes += len(s3_key.encode('utf-8'))
            if s3_key_bytes > max_s3_key_bytes:
                if len(capped_group) > 1:
                    yield capped_group
                return
            capped_group.append(s3_key)

        yield capped_group
        
    
def group_small_adjacent_partitions(s3_keys, max_row_count=PARQUET_FILE_MAX_DECISION_RECORDS):

    group = []
    for s3_key in s3_keys:
        if sum(map(row_count, group)) + row_count(s3_key) <= max_row_count:
            group.append(s3_key) # append to the previous group
        else:
            if len(group) >= 1: # in case row_count(s3_key) > max_row_count
                yield group
            group = [s3_key] # create a new group

    if len(group) >= 1:
        yield group
        
    
def merge_overlapping_adjacent_group_pairs(groups):
    
    # only merge single pairs of groups to keep row_count < max_row_count * 2
    candidate_group = None

    for group in groups:
        assert len(group) >= 1
        
        if candidate_group:
            if max(map(max_timestamp, candidate_group)) >= min(map(min_timestamp, group)):
                candidate_group.extend(group)
                yield candidate_group    
                candidate_group = None # only merge pairs, not unbounded continuous runs of groups
            else:
                yield candidate_group
                candidate_group = group
        else:
            candidate_group = group


def groom_handler(event, context):
    
    print(f'processing event {json_dumps(event)}')

    s3_keys = event['s3_keys']

    # load all s3_keys, merge records, and optionally split into multiple partitions
    RewardedDecisionPartition(model_name=event['model_name'], s3_keys=s3_keys).process()
    
    return None