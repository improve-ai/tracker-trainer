# Built-in imports
import io
import json
import gzip
import shutil
import re

# Local imports
import config


def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)


def copy_file(src, dest):
    ensure_parent_dir(dest)
    shutil.copy(src.absolute(), dest.absolute())


def save_gzipped_jsonlines(file, json_dicts):
    with gzip.open(file, mode='w') as gzf:
        for json_dict in json_dicts:
            gzf.write((json.dumps(json_dict) + '\n').encode())


def upload_gzipped_jsonlines(s3_bucket, s3_key, json_dicts):
    gzipped = io.BytesIO()

    save_gzipped_jsonlines(gzipped, json_dicts)

    gzipped.seek(0)

    config.s3client.put_object(Bucket=s3_bucket, Body=gzipped, Key=s3_key)


def delete_all(paths):
    for path in paths:
        path.unlink(missing_ok=True)


def find_first_gte(x, l):
    """
    Return the first element that's greater or equal than x.
    
    Modified from: https://stackoverflow.com/a/2236935/1253729
    """
    if x is None:
        return (None, None)

    return next(((i,v) for i,v in enumerate(sorted(l)) if v >= x), (None, None))


def list_s3_keys_containing(bucket_name, start_key, end_key):
    """
    Return a list of keys between the given `start_key` and `end_key` 
    in the given S3 `bucket_name`. 
    Both `start_key` and `end_key` are  inclusive but if there is no 
    match for `end_key`, return the closest key that's greater than 
    `end_key`. The comparisons of keys is done comparing the Unicode 
    code point numbers of individual characters.
    
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
    start_key : str
        Key from where to start the subset selection
    end_key : str
        The subset selection will be done up to this key or, if not 
        available, up to the closes key with a greater Unicode code 
        point number.

    Returns
    -------
    list
        A list of strings in the given S3 bucket found in the 
        given subset limits.

    Raises
    ------

    TypeError
        If something different than a str is passed to the start_key, end_key or bucket_name
    ValueError
        If `start_key` > `end_key`
    
    """    

    if not isinstance(bucket_name, str) or \
       not isinstance(start_key, str) or \
       not isinstance(end_key, str):
       raise TypeError

    if start_key > end_key:
        raise ValueError

    kwargs = { 'Bucket': bucket_name }

    keys = []
    while True:
        resp = config.s3client.list_objects_v2(**kwargs)
        
        if not 'Contents' in resp:
            return []

        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    if len(keys) == 0:
        return []

    keys.sort()

    # Find the index of the first key
    idx_start_gte, start_gte = find_first_gte(start_key, keys)

    # Find the index of the last key
    idx_end_gte, end_gte = find_first_gte(end_key, keys)

    # When the requested end key is beyond the available ones.
    #
    # Example:
    #
    # Keys: |||||||||||||||||||||||---------
    #       ^                             ^
    #      start                         end
    #
    if idx_start_gte is not None and idx_end_gte is None:
       result = keys

    # When the requested range has nothing 1.
    #
    # Example:
    #
    # Keys: ----------------------|||||||||||||||||||------
    #       ^                ^
    #   start/end         end/start
    #
    # No special care must be taken here because the default case will take care of it

    # When the requested range has nothing 2.
    #
    # Example:
    #
    # Keys: ----|||||||||||||||||||----------------
    #                                ^           ^
    #                            start/end    end/start
    #
    elif start_key > keys[-1] and end_key > keys[-1]:
        result = []

    else:
        result = keys[idx_start_gte:idx_end_gte+1]
    
    return result
