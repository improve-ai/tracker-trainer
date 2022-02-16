# Built-in imports
import gzip
from io import BytesIO

# External imports
import pandas as pd

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
