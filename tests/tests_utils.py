# Built-in imports
import gzip
from io import BytesIO
import itertools

# External imports
import pandas as pd
import portion as P
import orjson

# Local imports
import config


def dicts_to_df(dicts: list, columns: list = None, dtypes: dict = None):
    df = pd.DataFrame(dicts) if columns is None else pd.DataFrame(dicts, columns=columns)

    if dtypes is not None:
        df = df.astype(dtypes)
    return df


def upload_gzipped_jsonl_records_to_firehose_bucket(s3client, records):
    
    fileobj = BytesIO()
    with gzip.GzipFile(fileobj=fileobj, mode="wb") as gzf:
        for record in records:
            gzf.write(orjson.dumps(record) + b'\n')
    fileobj.seek(0)

    # Put some gzipped jsonl files in a bucket
    s3client.upload_fileobj(
        Fileobj   = fileobj,
        Bucket    = config.FIREHOSE_BUCKET,
        Key       = config.INCOMING_FIREHOSE_S3_KEY,
        ExtraArgs = { 'ContentType': 'application/gzip' }
    )


def get_unique_overlapping_keys(single_overlap_keys):
    return set(itertools.chain(*single_overlap_keys.values()))


def get_all_overlaps(keys_to_repair):
    # Create list of "Interval" objects
    train_s3_intervals = []
    for key in keys_to_repair:
        maxts_key, mints_key = key.split('/')[-1].split('-')[:2]
        interval = P.IntervalDict({P.closed(mints_key, maxts_key): [key]})
        train_s3_intervals.append(interval)

    # Modified from:
    # https://www.csestack.org/merge-overlapping-intervals/
    overlaps = [train_s3_intervals[0]]
    for i in range(1, len(train_s3_intervals)):

        pop_element = overlaps.pop()
        next_element = train_s3_intervals[i]

        if pop_element.domain().overlaps(next_element.domain()):
            new_element = pop_element.combine(next_element, how=lambda a, b: a + b)
            overlaps.append(new_element)
        else:
            overlaps.append(pop_element)
            overlaps.append(next_element)

    return overlaps
