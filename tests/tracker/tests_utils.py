# Built-in imports
from io import BytesIO
import os

# External imports
import orjson
import pandas as pd

# Local imports
import config
from src.ingest.firehose_record import DECISION_ID_KEY, MODEL_KEY
from src.ingest.partition import parquet_s3_key
from src.ingest.utils import is_valid_rewarded_decisions_s3_key


def dicts_to_df(dicts: list, columns: list = None, dtypes: dict = None):
    df = pd.DataFrame(dicts) if columns is None else pd.DataFrame(dicts, columns=columns)

    if dtypes is not None:
        df = df.astype(dtypes)
    return df


def load_ingest_test_case(test_case_file: str):
    test_case_path = os.sep.join([os.getenv('TEST_CASES_DIR'), 'tracker_test_cases', test_case_file])
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


def get_valid_s3_key_from_df(df, model_name):
    df.sort_values(DECISION_ID_KEY, inplace=True, ignore_index=True)
    return parquet_s3_key(
        model_name=model_name,
        min_decision_id=df[DECISION_ID_KEY].dropna().iloc[0],
        max_decision_id=df[DECISION_ID_KEY].dropna().iloc[-1], count=df.shape[0])


def get_valid_moto_s3_keys(parquet_files, model_names) -> list:

    test_cases_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_cases_dir is not None

    merge_test_data_relative_dir = os.getenv('MERGE_TEST_DATA_RELATIVE_DIR', None)
    assert merge_test_data_relative_dir is not None

    model_names_per_file = \
        [mn for pqf in parquet_files for mn in model_names if mn in pqf]

    valid_s3_keys = []

    for pq_file, model_name_for_pq_file in zip(parquet_files, model_names_per_file):
        pq_file_path = os.sep.join(
            [test_cases_dir, merge_test_data_relative_dir, pq_file])

        # load parquet file
        df = pd.read_parquet(pq_file_path)

        valid_s3_keys.append(get_valid_s3_key_from_df(df, model_name_for_pq_file))

    return valid_s3_keys


def get_model_name_from_env():
    model_name = os.getenv(MODEL_KEY, None)
    assert model_name is not None
    return model_name


def are_all_s3_keys_valid(s3_keys: list):
    assert len(s3_keys) > 0
    return all(is_valid_rewarded_decisions_s3_key(s3_key) for s3_key in s3_keys)
