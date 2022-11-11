# Built-in imports
from io import BytesIO

# External imports
import pandas as pd

# Local imports
import config


def dicts_to_df(dicts: list, columns: list = None, dtypes: dict = None):
    df = pd.DataFrame(dicts) if columns is None else pd.DataFrame(dicts, columns=columns)

    if dtypes is not None:
        df = df.astype(dtypes)
    return df


def upload_gzipped_records_to_firehose_bucket(s3client, path, key):

    s3client.upload_fileobj(
        Fileobj=BytesIO(open(path, 'rb').read()),
        Bucket=config.FIREHOSE_BUCKET,
        Key=key,
        ExtraArgs={'ContentType': 'application/gzip'}
    )
