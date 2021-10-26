# Built-in imports
import base64
import json
from datetime import datetime, timezone

# External imports
from dateutil.parser import parse, isoparse
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
    """ Return the min/max possible datetime representable with KSUIDs"""

    if min_or_max == "min":
        base62_str = MIN_STRING_ENCODED
    elif min_or_max =="max":
        base62_str = MAX_STRING_ENCODED

    ts_bytes = decodebytes(base62_str)[:TIME_STAMP_LENGTH]
    unixTime = int_from_bytes(ts_bytes, byteorder="big")
    
    ts = unixTime + EPOCH_TIME
    dt = datetime.utcfromtimestamp(ts)
    return dt


def get_min_max_payload_bytes(min_or_max):
    """ Return the min/max possible payload representable with KSUIDs """

    if min_or_max == "min":
        base62_str = MIN_STRING_ENCODED
        return b"\x00" * BODY_LENGTH
    elif min_or_max =="max":
        base62_str = MAX_STRING_ENCODED
        payload_bytes = decodebytes(base62_str)[TIME_STAMP_LENGTH:]
        return payload_bytes


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
    # if not isinstance(timestamp, int) or not isinstance(payload, bytes):
    #     raise TypeError
    
    # if (timestamp < 0) or (timestamp > MAX_INT_4_BYTES+EPOCH_TIME) or len(payload) > BODY_LENGTH:
    #     if timestamp < 0:
    #         print(f"timestamp < 0: {timestamp}")
    #     elif timestamp > MAX_INT_4_BYTES:
    #         print(f"timestamp > MAX_INT_4_BYTES: {timestamp}")
    #     elif len(payload) > BODY_LENGTH:
    #         print(f"len(payload) > BODY_LENGTH: {len(payload)}")
    #     raise ValueError
    
    ts_bytes = int_to_bytes(int(timestamp - EPOCH_TIME), 4,  "big")
    uid = list(ts_bytes) + list(payload)
    return encodebytes(bytes(uid))


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
            "ksuid_base62" : x.toBase62()
        }

        triplets.append(triplet)

    ##########################################################################
    # Add custom case for max timestamp and max payload
    ##########################################################################
    
    # Get max possible timestamp for KSUID
    dt = get_min_max_datetime("max").replace(tzinfo=timezone.utc)
    ts = int(dt.timestamp())

    # Get the max payload
    max_payload_bytes = get_min_max_payload_bytes("max")
    
    # Generate ksuid based on the above inputs
    ksuid_base62 = gen_ksuid(ts, max_payload_bytes)
    assert ksuid_base62 == MAX_STRING_ENCODED

    triplets.append({
        "timestamp" : dt.isoformat(),
        "uid_base64" : base64.b64encode(max_payload_bytes).decode("utf-8"),
        "ksuid_base62" : ksuid_base62
    })

    ##########################################################################
    # Add custom case for min timestamp and min payload
    ##########################################################################

    # Get min possible timestamp for KSUID
    dt = get_min_max_datetime("min").replace(tzinfo=timezone.utc)
    ts = int(dt.timestamp())

    # Get the min payload
    min_payload_bytes = get_min_max_payload_bytes("min")
    
    # Generate ksuid based on the above inputs
    ksuid_base62 = gen_ksuid(ts, min_payload_bytes)
    #assert ksuid_base62 == MIN_STRING_ENCODED

    triplets.append({
        "timestamp" : dt.isoformat(),
        "uid_base64" : base64.b64encode(min_payload_bytes).decode("utf-8"),
        "ksuid_base62" : ksuid_base62
    })

    ##########################################################################
    # Write triplets
    ##########################################################################
    with open("ksuid.json", "w") as f:
        json.dump(triplets, f, indent=4)
