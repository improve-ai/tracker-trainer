import json

from partition import list_partition_s3_keys
from utils import is_valid_model_name

def filter_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')

    model_name = event['model_name']
    assert is_valid_model_name(model_name)

    partition_s3_keys = list_partition_s3_keys(model_name)

    return partition_s3_keys
    
def groom_handler(event, context):
    
    print(f'processing event {json.dumps(event)}')
