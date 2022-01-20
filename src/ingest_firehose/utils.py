# Built-in imports
import re
import datetime
from dateutil import parser

# External imports
import orjson
from ksuid import Ksuid

# Local imports
from config import s3client


ZERO = datetime.timedelta(0)
REWARDED_DECISIONS_S3_KEY_REGEXP = r"rewarded_decisions/.+/parquet/\d{4}/\d{2}/\d{2}/\d{8}T\d{6}Z\-\d{8}T\d{6}Z\-(.){36}\.parquet"

class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return ZERO

utc = UTC()


def is_correct_s3_key(s3_key):
    """ Validate if an s3 key complies with the expected format """
    if re.match(REWARDED_DECISIONS_S3_KEY_REGEXP, s3_key):
        return True
    return False


def find_first_gte(x: str, l: list):
    """
    Return the index and value of the first element that's greater or 
    equal than x.

    Returns
    -------
    int or None
        Index of the first element that is greater or equal than x. None if no 
        element greater or equal than x can be found.
    str or None
        First element that is greater or equal than x. None if no element 
        greater or equal than x can be found.
    
    Modified from: https://stackoverflow.com/a/2236935/1253729
    """
    if x is None:
        return (None, None)

    return next(((i,v) for i,v in enumerate(sorted(l)) if v >= x), (None, None))


def list_s3_keys_containing(bucket_name, start_after_key, end_key, prefix='', valid_keys_only=True):
    """
    Return a list of keys between the given `start_after_key` and 
    `end_key` in the given S3 bucket (`bucket_name`). 
    `end_key` is  inclusive but if there is no match for `end_key`, 
    return the closest key that's greater than `end_key`. 
    The comparisons of keys is done comparing the Unicode code point 
    numbers of individual characters.
    
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
    start_after_key : str
        Key after which to start the subset selection
    end_key : str
        The subset selection will be done up to this key or, if not 
        available, up to the closes key with a greater Unicode code 
        point number.
    prefix : str
        Passed directly to the S3 call. Limits the response to keys 
        that begin with the specified prefix.
    valid_keys_only : bool
        Flag to check validity of s3 key format.
        Set to False in the tests to simplify the used cases.

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
       not isinstance(start_after_key, str) or \
       not isinstance(end_key, str) or \
       not isinstance(prefix, str):
       raise TypeError

    if start_after_key > end_key:
        raise ValueError

    kwargs = {
        'Bucket': bucket_name,
        'StartAfter': start_after_key,
        'Prefix': prefix
    }

    keys = []
    while True:
        resp = s3client.list_objects_v2(**kwargs)

        if 'Contents' not in resp:
            return []

        for obj in resp['Contents']:
            key = obj['Key']

            if (valid_keys_only and is_correct_s3_key(key)) or not valid_keys_only:
                keys.append(key)

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    keys.sort()

    if len(keys) == 0:
        return []

    # Find the index of the last key
    idx_end_gte, end_gte = find_first_gte(end_key, keys)

    if idx_end_gte is None:
        result = keys
    else:
        result = keys[:idx_end_gte+1]
    
    return result


def is_valid_model_name(model_name):   
    if not isinstance(model_name, str) \
            or len(model_name) == 0 \
            or len(model_name) > 64 \
            or not re.match('^[a-zA-Z0-9][\w\-.]{0,63}$', model_name):
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
