# Built-in imports
import base64
import json
from datetime import datetime, timezone, timedelta
import os

# External imports
from dateutil.parser import parse
from ksuid import ksuid
from ksuid.base62 import decodebytes, encodebytes
from ksuid.ksuid import TIME_STAMP_LENGTH, EPOCH_TIME, BODY_LENGTH
from ksuid.utils import int_from_bytes, int_to_bytes

MIN_STRING_ENCODED = "000000000000000000000000000"
MAX_STRING_ENCODED = "aWgEPTl1tmebfsQzFP4bxwgy80V" # base62

MAX_INT_4_BYTES = 4294967295

"""
About KSUID

KSUID has two fixed-length encodings:
  - A 20-byte binary encoding, composed of a 32-bit timestamp and a 
    128-bit randomly generated payload.
  - A 27-characters base62 encoding, which maps the lexicographic 
    ordering of ascii


This script generates a JSON file with a list of triplets: 

  - timestamp in iso 8601 format
  - uid of 128 bits base64 encoded
  - ksuid based on the two above

So that other part of the system can use it to, generate ksuids based 
on the timestamps and payloads and compare the results.

References: 
- [1] https://segment.com/blog/a-brief-history-of-the-uuid/

Note:
Using this script requires installing the master version found in 
github, not the one that gets installed with pip,

    pip install https://github.com/saresend/KSUID/archive/master.zip

"""


def get_min_max_datetime(min_or_max):
    """
    Return the min/max possible datetime representable with KSUIDs.
    
    This is really here only to show how to extract the timestamp from 
    a base62-encoded KSUID string, because it may just return one of 
    these:

        MIN_KSUID_DATETIME = parse("2014-05-13T16:53:20+00:00")
        MAX_KSUID_DATETIME = parse("2150-06-19T23:21:35+00:00")
    """

    assert min_or_max in ("min", "max")

    if min_or_max == "min":
        base62_str = MIN_STRING_ENCODED
    elif min_or_max =="max":
        base62_str = MAX_STRING_ENCODED

    ts_bytes = decodebytes(base62_str)[:TIME_STAMP_LENGTH]
    unixTime = int_from_bytes(ts_bytes, byteorder="big")
    
    ts = unixTime + EPOCH_TIME
    dt = datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
    return dt


def get_min_max_payload_bytes(min_or_max):
    """ Return the min/max possible payload representable with KSUIDs """

    if min_or_max == "min":
        return b"\x00" * BODY_LENGTH
    elif min_or_max =="max":
        return b"\xFF" * BODY_LENGTH


def gen_ksuid(timestamp, payload):
    """
    This function is basically the one from the ksuid package but 
    capable of receiving a custom payload.

    Parameters
    ----------
    timestamp: int
        a unix timestamp (4 bytes)
    payload: bytes
        a bytes object of length 16
    
    Returns
    -------
    str
        A base62-encoded ksuid string
    """
    
    ts_bytes = int_to_bytes(int(timestamp - EPOCH_TIME), 4,  "big")
    uid = list(ts_bytes) + list(payload)
    return encodebytes(bytes(uid))


def gen_custom_triplet(min_or_max, seconds):
    """
    Generate a triplet with a timestamp, uid and ksuid. Depending on
    the value of `min_or_max`, an extreme valid KSUID timestamp will
    be used with either the minimum or maximum value allowed by KSUID.
    Such extreme timestamp will be offset by `seconds` seconds.
    Depending on the value of `min_or_max`, the payload used will be
    either the minimum or maximum allowed by KSUID.

    Parameters
    ----------
    min_or_max : str
        Either "min" or "max"; used to select which KSUID timestamp 
        extreme to use in the triplet generation.
    seconds : int
        Number of seconds to offset the generated extreme timestamp

    Returns
    -------
    dict of str
        Triplet with timestamp, uid in base64 and ksuid in base62
    """

    assert min_or_max in ("min", "max")
    assert isinstance(seconds, int)

    # Get a timestamp second below the epoch
    extreme_dt = get_min_max_datetime(min_or_max)

    # Offset the datetime
    dt = extreme_dt + timedelta(seconds=seconds)
    
    # Simulate the clipping towards the min or max
    if (dt < get_min_max_datetime("min")) or (dt > get_min_max_datetime("max")):
        ts = int(extreme_dt.timestamp())
    else:
        ts = dt.timestamp()

    # The KSUID payload
    payload_bytes = get_min_max_payload_bytes(min_or_max)

    # Generate ksuid based on the above inputs
    ksuid_base62 = gen_ksuid(ts, payload_bytes)

    return {
        "timestamp" : dt.isoformat(),
        "uid_base64" : base64.b64encode(payload_bytes).decode("utf-8"),
        "ksuid_base62" : ksuid_base62.zfill(27)
    }


if __name__ == "__main__": 

    timestamps = [
        "2021-10-25T11:01:18",
        "2021-10-25T11:01:19",
        "2021-10-25T11:01:20",
        "2021-10-25T11:01:20Z",
        "2022-01-25T11:01:18Z",
    ]

    triplets = []

    for ts_str in timestamps:
        
        # Get a Unix timestamp
        unix_ts = parse(ts_str).replace(tzinfo=timezone.utc).timestamp()

        # Generate a KSUID
        x = ksuid(timestamp=unix_ts)
        
        # Get the internal 16 bytes payload as a list of int
        uid_bytes_as_int_list = x.getPayload()

        # Get the payload as a bytes object
        uid_bytes = bytes(uid_bytes_as_int_list)

        # Encode the payload in base64
        b64_uid = base64.b64encode(uid_bytes).decode("utf-8")

        triplet = {
            "timestamp" : ts_str,
            "uid_base64" : b64_uid,
            "ksuid_base62" : (x.toBase62()).zfill(27)
        }

        triplets.append(triplet)


    ##########################################################################
    # Custom cases around the min timestamp
    ##########################################################################
    triplet = gen_custom_triplet(min_or_max="min", seconds=-1)
    assert triplet.get('ksuid_base62') == MIN_STRING_ENCODED
    triplets.append(triplet)
    
    triplet = gen_custom_triplet(min_or_max="min", seconds=0)
    assert triplet.get('ksuid_base62') == MIN_STRING_ENCODED
    triplets.append(triplet)
    
    triplets.append(gen_custom_triplet(min_or_max="min", seconds=1))


    ##########################################################################
    # Custom cases around the max timestamp
    ##########################################################################
    triplets.append(gen_custom_triplet(min_or_max="max", seconds=-1))

    triplet = gen_custom_triplet(min_or_max="max", seconds=0)
    assert triplet.get('ksuid_base62') == MAX_STRING_ENCODED
    triplets.append(triplet)

    triplet = gen_custom_triplet(min_or_max="max", seconds=1)
    assert triplet.get('ksuid_base62') == MAX_STRING_ENCODED
    triplets.append(triplet)


    ##########################################################################
    # Write triplets
    ##########################################################################
    with open("ksuid.json", "w") as f:
        json.dump(triplets, f, indent=4)
