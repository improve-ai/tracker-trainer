# Built-in imports
from io import BytesIO
import os

# External imports
import orjson
import pandas as pd

# Local imports
import config


def dicts_to_df(dicts: list, columns: list = None, dtypes: dict = None):
    df = pd.DataFrame(dicts) if columns is None else pd.DataFrame(dicts, columns=columns)

    if dtypes is not None:
        df = df.astype(dtypes)
    return df


def load_ingest_test_case(test_case_file: str):
    test_case_path = os.sep.join([os.getenv('TEST_CASES_DIR'), test_case_file])
    with open(test_case_path, 'r') as tcf:
        test_case_json = orjson.loads(tcf.read())
    return test_case_json


def upload_gzipped_records_to_firehose_bucket(s3_client, path, key):

    s3_client.upload_fileobj(
        Fileobj=BytesIO(open(path, 'rb').read()),
        Bucket=config.FIREHOSE_BUCKET,
        Key=key,
        ExtraArgs={'ContentType': 'application/gzip'}
    )
