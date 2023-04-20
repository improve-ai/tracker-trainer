# Built-in imports
import json
import numpy as np
import os
from pytest import raises

import config
import firehose_record
import partition
import utils
from ingest_firehose import lambda_handler, RECORDS_KEY, S3_KEY, BUCKET_KEY, \
    NAME_KEY, OBJECT_KEY, KEY_KEY

parquet_s3_key = partition.parquet_s3_key


from tracker.tests_utils import upload_gzipped_records_to_firehose_bucket

ENGINE = 'fastparquet'


def _read_test_case(test_case_json_filename):
    test_data_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_data_dir is not None
    test_case_path = os.sep.join([test_data_dir, 'tracker_test_cases', test_case_json_filename])
    with open(test_case_path, 'r') as tcf:
        test_case = json.loads(tcf.read())
    return test_case


def _unpack_ingest_test_case_json(test_case_envar):
    test_case_filename = os.getenv(test_case_envar, None)
    test_case = _read_test_case(test_case_filename)

    records_file = test_case.get("records_file", None)
    assert records_file is not None

    test_data_dir = os.getenv('TEST_CASES_DIR', None)
    assert test_data_dir is not None
    path = os.sep.join([test_data_dir, 'data', 'ingest', records_file])

    firehose_s3_key_prefix = test_case.get("firehose_s3_key_prefix", None)
    assert firehose_s3_key_prefix is not None

    expected_keys = test_case.get("expected_train_files", None)
    assert expected_keys is not None

    return path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir


def _prepare_s3(s3client, path, firehose_s3_key_prefix, records_file):
    config.s3client = \
        utils.s3client = \
        partition.s3client = \
        firehose_record.s3client = s3client
    s3client.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3client.create_bucket(Bucket=config.TRAIN_BUCKET)

    key = os.sep.join([firehose_s3_key_prefix, records_file])
    upload_gzipped_records_to_firehose_bucket(s3client, path, key)
    return key


def _validate_ingest(s3client, expected_keys):
    response = s3client.list_objects_v2(Bucket=config.TRAIN_BUCKET, Prefix='')
    keys_in_bucket = [s3file['Key'] for s3file in response['Contents']]
    calculated_filenames_chunks = ['-'.join(key.split('/')[-1].split('-')[:3]) for key in keys_in_bucket]
    calculated_prefixes = [os.sep.join(key.split('/')[:-1]) for key in keys_in_bucket]

    expected_filenames_chunks = ['-'.join(key.split('/')[-1].split('-')[:3]) for key in expected_keys]
    expected_prefixes = [os.sep.join(key.split('/')[:-1]) for key in expected_keys]

    np.testing.assert_array_equal(calculated_filenames_chunks, expected_filenames_chunks)
    np.testing.assert_array_equal(calculated_prefixes, expected_prefixes)


def test_successful_ingest(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    key = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)

    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: key}
            }}
        ]
    }

    lambda_handler(event, None)
    _validate_ingest(s3, expected_keys)


def test_ingest_raises_for_no_records_in_event(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    _ = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {RECORDS_KEY: []}

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_more_than_1_records(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    _ = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {RECORDS_KEY: [1, 2, 3]}

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_s3_key_not_in_records(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    key = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {
        RECORDS_KEY: [
            {"dummy-key": {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_bucket_key_not_in_s3_key_dict(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    key = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                'dummy-bucket-key': {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_object_key_not_in_s3_key_dict(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    key = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                "dummy-object-key": {KEY_KEY: key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_name_key_not_in_bucket_dict(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    key = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {"dummy-name-key": config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_s3_key_not_in_object_dict(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    key = _prepare_s3(s3, path, firehose_s3_key_prefix, records_file)
    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {"dummy-key": key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_missing_s3_file(s3):
    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    config.s3client = \
        utils.s3client = \
        partition.s3client = \
        firehose_record.s3client = s3
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)
    key = os.sep.join([firehose_s3_key_prefix, records_file])

    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_missing_firehose_bucket(s3):
    config.s3client = \
        utils.s3client = \
        partition.s3client = \
        firehose_record.s3client = s3
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)

    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: 'dummy-value'}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)


def test_ingest_raises_for_missing_train_bucket(s3):

    path, records_file, firehose_s3_key_prefix, expected_keys, test_data_dir = \
        _unpack_ingest_test_case_json(test_case_envar='TEST_CASE_INGEST_JSON')
    config.s3client = \
        utils.s3client = \
        partition.s3client = \
        firehose_record.s3client = s3
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)

    key = os.sep.join([firehose_s3_key_prefix, records_file])

    event = {
        RECORDS_KEY: [
            {S3_KEY: {
                BUCKET_KEY: {NAME_KEY: config.FIREHOSE_BUCKET},
                OBJECT_KEY: {KEY_KEY: key}
            }}
        ]
    }

    with raises(Exception) as exc:
        lambda_handler(event, None)
