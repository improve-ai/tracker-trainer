# Built-in imports
from concurrent.futures import ThreadPoolExecutor
import importlib
from io import BytesIO
import json
import os
import re
import sys
import time

# External imports
import itertools  # needed for surrogate function
import numpy as np
import pandas as pd
import portion as P  # needed for surrogate function
import pytest

# Local imports
import config
import firehose_record
import rewarded_decisions
from rewarded_decisions import repair_overlapping_keys, get_all_overlaps, get_unique_overlapping_keys, \
    DECISION_ID_KEY, DF_SCHEMA
import utils
import worker

s3_key = rewarded_decisions.s3_key

from tests_utils import dicts_to_df, upload_gzipped_jsonl_records_to_firehose_bucket


ENGINE = 'fastparquet'


def test_worker_ingestion_fail_due_to_bad_records(s3, get_decision_rec, get_rewarded_decision_rec):
    """
    Ensure that:

    - When a Parquet file in the train bucket has invalid records, an 
      exception is raised and the whole job fails.
    - Parquet file is written to unrecoverable/...
    - Original faulty parquet file was deleted

    Parameters:
        s3: pytest fixture
        get_decision_rec: custom pytest fixture
        get_rewarded_decision_rec: custom pytest fixture
    """

    # Replace the s3client with a mocked one
    config.s3client = firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

    # Create mocked buckets
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)

    ##########################################################################
    # Create Parquet file for the train bucket
    ##########################################################################

    # decision_ids range covered by a Partition in the train bucket
    min_decision_id = '111111111000000000000000000'
    max_decision_id = '111111119000000000000000000'
    
    # Create RDRs with None decision_ids
    rdr1 = get_rewarded_decision_rec(decision_id=min_decision_id)
    rdr1[DECISION_ID_KEY] = None
    rdr2 = get_rewarded_decision_rec(decision_id=max_decision_id)
    rdr2[DECISION_ID_KEY] = None

    # Upload parquet file made of the above RDRs
    model_name = 'test-model-name-1.0'
    parquet_key = s3_key(model_name, min_decision_id, max_decision_id)
    df = dicts_to_df(dicts=[rdr1, rdr2], columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    df.to_parquet(f's3://{config.TRAIN_BUCKET}/{parquet_key}', engine=ENGINE, compression='ZSTD', index=False)

    # Ensure the key is really there
    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{model_name}')
    all_keys = [x['Key'] for x in response['Contents']]
    assert parquet_key in all_keys


    ##########################################################################
    # Create JSONL files for the Firehose bucket
    ##########################################################################
    record1_decision_id = '111111111000000000000000001'
    assert min_decision_id <= record1_decision_id <= max_decision_id
    record2_decision_id = '111111111000000000000000002'
    assert min_decision_id <= record2_decision_id <= max_decision_id

    # Create decision records with decision_ids falling somewhere between the above min/max
    record1 = get_decision_rec(msg_id_val=record1_decision_id)
    record2 = get_decision_rec(msg_id_val=record2_decision_id)   
    upload_gzipped_jsonl_records_to_firehose_bucket(s3, [record1, record2])


    ##########################################################################
    # Validations
    ##########################################################################

    # Ensure the whole work fails when an invalid parquet file is encountered
    with pytest.raises(ValueError):
        worker.worker()


    # Ensure the parquet file is in the unrecoverable path
    unrecoverable_key = f'unrecoverable/{parquet_key}'
    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'unrecoverable/')
    all_keys = [x['Key'] for x in response['Contents']]
    assert unrecoverable_key in all_keys


    # Ensure the original faulty parquet file got deleted
    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{model_name}')
    assert 'Contents' not in response # No 'Content's == no files


def get_test_case_json(test_case_json_path: str):
    test_case_dir = os.getenv('TEST_CASES_DIR')
    test_case_path = os.sep.join([test_case_dir, test_case_json_path])
    with open(test_case_path) as tcf:
        test_case = json.loads(tcf.read())
    return test_case


def prepare_buckets(s3, test_case_json):
    # it is ok to upload all files at once because here we manually trigger worker
    # (AWS trigger listens for upload event)
    # Create mocked buckets
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)

    # get test case def
    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    # get ingested files paths
    input_files = \
        [os.sep.join([os.getenv('HOME_DIR'), input_file]) for input_file
         in test_case.get('input_jsonlines_files', None) if input_file]
    assert input_files is not None \
           and not any([input_file is None for input_file in input_files])

    # get ingested files s3 keys
    firehose_bucket_s3_keys = test_case.get('firehose_bucket_s3_keys', None)
    assert firehose_bucket_s3_keys is not None \
           and not any([s3_key is None for s3_key in firehose_bucket_s3_keys])

    assert len(input_files) == len(firehose_bucket_s3_keys)

    # get model name for ingested data
    model_name = test_case.get('model_name', None)
    assert model_name is not None

    os.environ['model_name'] = model_name

    # load input jsonline
    for jsonlines_gzip_path, firehose_s3_key in zip(input_files, firehose_bucket_s3_keys):

        # upload single gz file to bucket
        s3.put_object(
            Body=open(jsonlines_gzip_path, 'rb'),
            Bucket=config.FIREHOSE_BUCKET,
            Key=firehose_s3_key)

    # assert that files have been loaded to s3
    files_in_firehose_bucket = \
        [entry['Key'] for entry in s3.list_objects_v2(Bucket=config.FIREHOSE_BUCKET)['Contents']]

    np.testing.assert_array_equal(files_in_firehose_bucket, sorted(firehose_bucket_s3_keys))


def _get_timestamps_and_uuid4_chunks(name: str):
    split_filename = name.split('-')
    timestamps = '-'.join(split_filename[:2])
    uuid = '-'.join(split_filename[2:]).replace('.parquet', '')
    return timestamps, uuid


def check_s3_key_against_parquet_path(s3_key: str, local_fs_path: str):
    # extract timestamps to check if timespan is equal
    s3_key_timestamps, s3_key_uuid = _get_timestamps_and_uuid4_chunks(name=s3_key)
    fs_path_timestamps, _ = _get_timestamps_and_uuid4_chunks(name=local_fs_path)

    assert s3_key_timestamps == fs_path_timestamps
    assert re.search('[a-zA-Z0-9\-]{36}', s3_key_uuid)


def _get_df_from_s3(s3_key: str, s3):
    object_response = s3.get_object(Bucket=config.TRAIN_BUCKET, Key=s3_key)
    parquet_bytes = object_response['Body'].read()
    return pd.read_parquet(BytesIO(bytes(parquet_bytes)))


def ensure_results_are_correct(s3, test_case_json: dict):

    expected_output = test_case_json.get('test_output', None)
    assert expected_output is not None

    expected_train_s3_keys = expected_output.get('expected_train_s3_keys', None)
    assert expected_train_s3_keys is not None

    s3_keys_in_train_bucket = \
        [entry['Key'] for entry in s3.list_objects_v2(Bucket=config.TRAIN_BUCKET)['Contents']]

    # check that all fetched keys are correct
    assert all([utils.is_valid_rewarded_decisions_s3_key(s3_key) for s3_key in s3_keys_in_train_bucket])

    # check that there is no overlap
    overlaps = get_all_overlaps(keys_to_repair=s3_keys_in_train_bucket)
    for overlap in overlaps:
        assert len(get_unique_overlapping_keys(overlap)) == 1

    datetimes_from_keys = \
        [dt for s3_key in s3_keys_in_train_bucket for dt in _get_timestamps_and_uuid4_chunks(s3_key)[0]]

    expected_parquet_files = sorted(expected_output.get('expected_parquet_files', None))
    datetimes_from_parquet_files = \
        [dt for fs_path in s3_keys_in_train_bucket for dt in _get_timestamps_and_uuid4_chunks(fs_path)[0]]

    # check that max and min datetimes are identical
    assert max(datetimes_from_keys) == max(datetimes_from_parquet_files) and \
           min(datetimes_from_keys) == min(datetimes_from_parquet_files)

    # check_that_concat frames are equal
    s3_df = \
        pd.concat([_get_df_from_s3(s3_key=s3_key, s3=s3) for s3_key in s3_keys_in_train_bucket])\
        .reset_index(drop=True)

    expected_df = pd.read_parquet(expected_parquet_files)

    pd.testing.assert_frame_equal(s3_df, expected_df)


def test_single_successful_ingest(s3, **kwargs):
    # Replace the s3client with a mocked one
    config.s3client = firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

    test_case_json = get_test_case_json(os.getenv('TEST_CASE_SINGLE_FILE_INGEST_JSON'))
    # Create and populated mocked buckets
    prepare_buckets(s3, test_case_json)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    ingested_s3_keys = test_case.get('firehose_bucket_s3_keys', None)
    assert ingested_s3_keys is not None

    for s3_key in ingested_s3_keys:
        # set S3 key
        os.environ['S3_KEY'] = s3_key
        config.INCOMING_FIREHOSE_S3_KEY = s3_key
        worker.INCOMING_FIREHOSE_S3_KEY = s3_key

        # run worker
        worker.worker()

    # check data against reference df
    ensure_results_are_correct(s3, test_case_json)


def run_worker_with_s3_key(s3_key, attempt, s3):
    # multiprocessing.Process won't work with moto
    # threading.Thread uses same env as parent process
    # worker module must be 're-imported' so different values of INCOMING_FIREHOSE_S3_KEY
    # are persisted for separate jobs

    spec_config = importlib.util.find_spec('config')
    config = importlib.util.module_from_spec(spec_config)
    spec_config.loader.exec_module(config)
    sys.modules['config'] = config
    config.ATTEMPT = attempt
    config.s3client = s3

    spec_worker = importlib.util.find_spec('worker')
    worker = importlib.util.module_from_spec(spec_worker)
    spec_worker.loader.exec_module(worker)
    sys.modules['worker'] = worker
    worker.INCOMING_FIREHOSE_S3_KEY = s3_key
    worker.s3client = s3

    spec_rewarded_decisions = importlib.util.find_spec('rewarded_decisions')
    rewarded_decisions = importlib.util.module_from_spec(spec_rewarded_decisions)
    spec_rewarded_decisions.loader.exec_module(rewarded_decisions)
    sys.modules['rewarded_decisions'] = rewarded_decisions
    rewarded_decisions.s3client = s3

    spec_firehose_record = importlib.util.find_spec('firehose_record')
    firehose_record = importlib.util.module_from_spec(spec_firehose_record)
    spec_firehose_record.loader.exec_module(firehose_record)
    sys.modules['firehose_record'] = firehose_record
    firehose_record.s3client = s3

    # run worker within try-except clause to return info about failed jobs
    try:
        worker.worker()
    except Exception as exc:
        print(f'## ERROR during: {attempt} attempt {exc}###')

        return 1
    return 0


def run_batch_ingest_with_threads(s3_keys, s3, ingest_interval: int = None):

    with ThreadPoolExecutor(max_workers=len(s3_keys)) as executor:
        futures = {}
        for s3_key in s3_keys:
            future = executor.submit(run_worker_with_s3_key, s3_key, 1, s3)
            futures[s3_key] = {'future': future, 'attempts': 1}
            if ingest_interval is not None and ingest_interval > 0:
                wait_time = ingest_interval + np.random.rand() / 10
                print(f'Waiting {wait_time} seconds before next ingest')
                time.sleep(wait_time)

        while True:
            # check which thread has finished
            finished_threads = [k for k, v in futures.items() if not v['future'].running()]

            if not finished_threads:
                # if all workers are still running wait
                time.sleep(1)
                continue

            finished_threads_results = \
                {k: futures[k]['future'].result() for k in finished_threads}

            if all([r == 0 for r in finished_threads_results.values()]) \
                    and len(finished_threads_results) == len(futures):
                break

            failed_threads = 0
            for s3_key, result in finished_threads_results.items():
                if result == 1:
                    failed_threads += 1
                    # restart thread
                    futures[s3_key]['attempts'] += 1
                    if futures[s3_key]['attempts'] > 5:
                        raise RuntimeError(f'Exceeded 5 retries for worker {s3_key}')
                    threads_running = len(futures) - len([k for k, v in futures.items() if not v['future'].running()])
                    print(f'### THREADS RUNNING {threads_running} ###')
                    future = \
                        executor.submit(
                            run_worker_with_s3_key, s3_key, futures[s3_key]['attempts'], s3)
                    futures[s3_key]['future'] = future
                    time.sleep(0.1)

            print(f'### FAILED THREADS: {failed_threads} ###')
            time.sleep(1)


def test_successful_batch_ingest(s3, **kwargs):
    # Replace the s3client with a mocked one
    config.s3client = firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

    test_case_json = get_test_case_json(os.getenv('TEST_CASE_BATCH_INGEST_JSON'))

    # Create and populated mocked buckets
    prepare_buckets(s3, test_case_json)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    ingested_s3_keys = test_case.get('firehose_bucket_s3_keys', None)

    run_batch_ingest_with_threads(s3_keys=ingested_s3_keys, s3=s3)

    # check data against reference df
    ensure_results_are_correct(s3, test_case_json)


def test_successful_every_n_seconds_ingest(s3, **kwargs):
    # Replace the s3client with a mocked one
    config.s3client = firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

    ingest_interval = int(os.getenv('TEST_INGEST_EVERY_N_SECONDS'))

    test_case_json = get_test_case_json(os.getenv('TEST_CASE_BATCH_INGEST_JSON'))

    # Create and populated mocked buckets
    prepare_buckets(s3, test_case_json)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    ingested_s3_keys = test_case.get('firehose_bucket_s3_keys', None)

    run_batch_ingest_with_threads(s3_keys=ingested_s3_keys, ingest_interval=ingest_interval, s3=s3)

    # check data against reference df
    ensure_results_are_correct(s3, test_case_json)


def test_successful_batch_after_batch_ingest(s3, **kwargs):
    config.s3client = firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

    test_case_json = get_test_case_json(os.getenv('TEST_CASE_BATCH_INGEST_JSON'))

    # Create and populated mocked buckets
    prepare_buckets(s3, test_case_json)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    ingested_s3_keys = test_case.get('firehose_bucket_s3_keys', None)

    s3_keys_batches = [list(batch) for batch in np.split(np.array(ingested_s3_keys), 2)]
    for s3_keys_batch in s3_keys_batches:
        run_batch_ingest_with_threads(s3_keys=s3_keys_batch, s3=s3)
        print('### Waiting 15 seconds before second batch ingest ###')
        time.sleep(15)

    # check data against reference df
    ensure_results_are_correct(s3, test_case_json)


def test_successful_batch_reingest(s3, **kwargs):
    config.s3client = firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

    test_case_json = get_test_case_json(os.getenv('TEST_CASE_BATCH_INGEST_JSON'))

    # Create and populated mocked buckets
    prepare_buckets(s3, test_case_json)

    test_case = test_case_json.get('test_case', None)
    assert test_case is not None

    ingested_s3_keys = test_case.get('firehose_bucket_s3_keys', None)

    # initial ingest
    run_batch_ingest_with_threads(s3_keys=ingested_s3_keys, s3=s3)

    print('Waiting 10 seconds before batch reingest')
    time.sleep(15)

    reingested_keys = \
        list(np.random.choice(ingested_s3_keys, int(len(ingested_s3_keys) / 2), replace=False))
    run_batch_ingest_with_threads(s3_keys=reingested_keys, s3=s3)

    # check data against reference df
    ensure_results_are_correct(s3, test_case_json)
