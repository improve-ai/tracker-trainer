from config import PARQUET_FILE_MAX_DECISION_RECORDS, MAX_GROOM_ITERATIONS
from datetime import datetime, timedelta
from partition import RewardedDecisionPartition, list_partition_s3_keys, min_timestamp, max_timestamp, row_count, \
    min_max_timestamp_row_count, ISO_8601_BASIC_FORMAT
from utils import is_valid_model_name, json_dumps

CHECK_S3_KEYS_FOR_OVRELAPS = True


def filter_handler(event, context):
    """
    The filter process determines which rewarded decision record partitions should be merged prior to training.
    
    The grouping process relies on S3's behavior of listing keys in lexicographical order. Since partitions begin
    with a timestamp of the max decision id, the S3 keys are sorted ascending by maximum decision id.
    
    The first level of grouping is to group adjacent S3 keys in groups of up to 10,000 cumulative records and up to 500
    keys.
    
    The second level of grouping is to group together single pairs of adjacent groups that contain overlapping minimum and
    maximum timestamp values. Only single pairs are merged together to ensure the total record count of the group does not
    exceed 20,000 cumulative records or 1000 keys.  We do not combine groups of more than 1000 keys due to S3 being unable to
    delete more than 1000 keys in a single DeleteObjects operation. Any group containing more than 10000 keys is due to
    overlapping keys and will trigger dispersion of multiple partitions throughout the timeline index using the logic defined
    in partition.maybe_split_on_timestamp_boundaries
    
    At this point, any groups containing only a single key are filtered out, nothing needs to be done with them.
    
    Finally, the total bytes of the key strings are capped to stay under the Step Function payload limit of 256KB.
    
    By default, the filter process will stop returning groups after 10 iterations.
    
    Args:
        model_name: determines the S3 prefix to process within the train bucket
        iteration: the current iteration count of the groom process
        
    Return:
        iteration: the incremented iteration count
        groom_groups: an array of arrays of S3 keys to merge together
    """
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

    # TODO this might be the place to validate that there is no overlap
    # if groups is an empty list this indicates that current code did not detect
    # any overlaps to fix and  s3_keys can be checked for overlaps; If groups
    # are not empty this means that there are overlaps detected by the current
    # code and they will be repaired by the groom process
    if not groups and CHECK_S3_KEYS_FOR_OVRELAPS:
        # assert that there is no overlap between s3_keys
        assert_no_overlapping_keys(s3_keys)

    return {'iteration': iteration, 'groom_groups': list(groups)} # wrap in list for JSON serializiation
    

def assert_no_overlapping_keys(s3_keys):
    # extract desired data from s3 keys
    s3_keys_infos = [min_max_timestamp_row_count(s3_key) for s3_key in s3_keys]
    # sort keys by max dates ascending
    sorted_s3_keys_infos = sorted(s3_keys_infos, key=lambda x: datetime.strptime(x[1], ISO_8601_BASIC_FORMAT))

    min_datetimes = [datetime.strptime(info[0], ISO_8601_BASIC_FORMAT) for info in sorted_s3_keys_infos]
    max_datetimes = [datetime.strptime(info[1], ISO_8601_BASIC_FORMAT) for info in sorted_s3_keys_infos]

    # timedelta() defaults to 0 seconds
    # if any max datetime is greater than min datetime of next s3 key this is considered an overlap
    assert all([
        (min_datetimes[i + 1] - max_datetimes[i]) > timedelta() for i in range(len(max_datetimes) - 1)]), \
        'Overlapping keys detected at and at this point they should have been merged'


def group_partitions_to_groom(s3_keys):
    
    # group into no more than 500 items as merge overlapping may double the size
    # and the maximum group size for groom is 1000
    groups = group_small_adjacent_partitions(s3_keys, max_group_size=500)

    # may double the group size up to 1000 partitions with up to 2 * PARQUET_FILE_MAX_DECISION_RECORDS rows
    groups = merge_overlapping_adjacent_group_pairs(groups)
    
    # filter out single s3_keys that don't need to be merged
    groups = filter(lambda x: len(x) > 1, groups)
    
    # the maximum step function payload is 256KB so cap the yielded key bytes
    return cap_s3_key_bytes(groups)
    

def group_small_adjacent_partitions(s3_keys, max_row_count=PARQUET_FILE_MAX_DECISION_RECORDS, max_group_size=500):

    group = []
    for s3_key in s3_keys:
        if sum(map(row_count, group)) + row_count(s3_key) <= max_row_count \
        and len(group) < max_group_size:
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

    # TODO unit test all cases for last candidate groups
    if candidate_group:
        yield candidate_group
        

def cap_s3_key_bytes(groups, max_s3_key_bytes=204800):
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


def groom_handler(event, context):
    """
    The groom task takes a single group of keys, loads them, merges them together, and if they are larger than 10,000 records, disperses
    multiple partitions throughout the timeline index using the logic defined in partition.maybe_split_on_timestamp_boundaries(). Upon
    successful completion of this process all input s3_keys are deleted.

    Args:
        model_name: the name of the model to determine the S3 key prefix
        s3_keys: the s3_keys to load, merge, save, delete

    Return: None
    """

    print(f'processing event {json_dumps(event)}')

    s3_keys = event['s3_keys']

    assert len(s3_keys) <= 1000

    # load all s3_keys, merge records, and optionally split into multiple partitions
    RewardedDecisionPartition(model_name=event['model_name'], s3_keys=s3_keys).process()

    return None