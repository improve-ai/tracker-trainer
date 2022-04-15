# Built-in imports
import datetime
from dateutil import parser
import re

# External imports
from ksuid import Ksuid
import orjson

# Local imports
from config import s3client, stats
from constants import MODEL_NAME_REGEXP, REWARDED_DECISIONS_S3_KEY_REGEXP

ZERO = datetime.timedelta(0)

class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return ZERO

utc = UTC()


def list_s3_keys(bucket_name, prefix='', after_key=''):
    """
    Return a lexicographically sorted list of keys after the given `key`.
    
    Listing S3's keys returns them in UTF8-binary order:
        "List results are always returned in UTF-8 binary order". [3]
    The same occurs with Python:
        "The comparison uses lexicographical ordering. (...) 
        Lexicographical ordering for strings uses the Unicode code point 
        number to order individual characters". [4]

    References:
    [1] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    [2] https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    [3] https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html
    [4] https://docs.python.org/3/tutorial/datastructures.html#comparing-sequences-and-other-types


    Parameters
    ----------
    bucket_name : str
        AWS S3 bucket name
    key : str
        Key after which to start the subset selection
    prefix : str
        Passed directly to the S3 call. Limits the response to keys 
        that begin with the specified prefix.

    Returns
    -------
    list
        A list of strings in the given S3 bucket found in the 
        given subset limits.

    Raises
    ------

    TypeError
        If something different than a str is passed to the start_after_key, 
        end_key or bucket_name
    ValueError
        If `start_after_key` > `end_key`
    
    """  

    if not isinstance(bucket_name, str) or \
       not isinstance(after_key, str) or \
       not isinstance(prefix, str):
        raise TypeError

    kwargs = {
        'Bucket': bucket_name,
        'StartAfter': after_key,
        'Prefix': prefix
    }

    keys = []
    while True:
        resp = s3client.list_objects_v2(**kwargs)
        stats.increment_s3_requests_count('list')

        if 'Contents' not in resp:
            return []

        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


def list_partitions_after(bucket_name, key, prefix='', valid_keys_only=True):
    keys = list_s3_keys_after(bucket_name=bucket_name, key=key, prefix=prefix)
    return keys if not valid_keys_only else [k for k in keys if is_valid_rewarded_decisions_s3_key(k)]


def is_valid_rewarded_decisions_s3_key(s3_key):
    """ Validate if an s3 key complies with the expected format """
    if re.match(REWARDED_DECISIONS_S3_KEY_REGEXP, s3_key):
        return True
    return False


def is_valid_model_name(model_name):   
    if not isinstance(model_name, str) \
            or len(model_name) == 0 \
            or len(model_name) > 64 \
            or not re.match(MODEL_NAME_REGEXP, model_name):
        return False
        
    return True

    
def is_valid_ksuid(id_):
    
    if not isinstance(id_, str):
        return False
    
    if len(id_) != 27:
        return False
    
    try:
        # Disallow KSUIDs from the future, otherwise it could severely hurt
        # the performance of the partitions by creating a huge partition in the future
        # that new records keep aggregating into. At some point that partition would
        # no longer fit in RAM and processing could seize.
        if Ksuid.from_base62(id_).datetime > datetime.datetime.now(datetime.timezone.utc):
            return False
    except:
        # there was an exception parsing the KSUID, fail
        return False
        
    return True


def get_valid_timestamp(timestamp):
    """ Return a parsed and validated timestamp"""
    
    # if missing / None raise
    assert timestamp is not None
    
    # if not string raise
    assert isinstance(timestamp, str)
    
    # check string format
    try:
        parsed_timestamp = parser.isoparse(timestamp)
    except ValueError:
        parsed_timestamp = None
    
    assert parsed_timestamp is not None

    return parsed_timestamp
    

def json_dumps(val):
    # sorting the json keys may improve compression
    return orjson.dumps(val, option=orjson.OPT_SORT_KEYS).decode("utf-8")
    

def json_dumps_wrapping_primitive(val):
    """
    Wrapping ensures that even null JSON values are always persisted as a dictionary
    """
    if val is None:
        # "wrap" null primitive values as an empty dictionary.  This is the only way that null givens should be encoded
        # but it is also fine for null variants, samples, and runners_up items.
        val = {}
    elif not (isinstance(val, dict) or isinstance(val, list)):
        # The feature encoder treats JSON '<primitive>'' and '{ "$value": <primitive> }' identically.
        # Note that it would also be fine to wrap null/None values in this way, though it would not be technically
        # correct for the givens as it does not accept primitive values and givens are feature-encoded differently than
        # variants.
        val = { '$value': val }
    
    return json_dumps(val)
