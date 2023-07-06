# Built-in imports
import os

from pytest import raises

# Local imports
import src.ingest.config

src.ingest.config.FIREHOSE_BUCKET = os.getenv('FIREHOSE_BUCKET', None)
assert src.ingest.config.FIREHOSE_BUCKET is not None

src.ingest.config.TRAIN_BUCKET = os.getenv('TRAIN_BUCKET', None)
assert src.ingest.config.TRAIN_BUCKET is not None


from src.ingest.groom import assert_no_overlapping_keys


ALL_OVERLAPPING_S3_KEYS = [
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135416Z-20230705T135106Z-82-177d369c-5aa0-4a8c-96a9-84652888acb8.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135246Z-20230705T135106Z-81-799ba78b-bfb6-4ef6-a0b6-6cb6463ad7e4.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135116Z-20230705T135106Z-50-5bbb3f4c-1034-4d0c-89dd-50b15e6dc243.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135546Z-20230705T135106Z-80-0d0dee71-7b1e-40a6-ab23-d05ba646afad.parquet']

NO_OVERLAPPING_S3_KEYS = [
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135546Z-20230705T135106Z-200-569cb372-0408-4fff-beb4-33048ac9d4df.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T124005Z-20230705T113025Z-10050-d29a35cc-2526-4c2b-aa24-91026a46d23c.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T112905Z-20230705T112425Z-200-cca7aad5-1313-49cf-af2b-e15a885e62ad.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T140527Z-20230705T135706Z-10050-9ab23c97-5886-4f26-bf4f-1c3bf0027da4.parquet'
]

TWO_OVERLAPPING_S3_KEYS = [
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135246Z-20230705T135106Z-81-799ba78b-bfb6-4ef6-a0b6-6cb6463ad7e4.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135116Z-20230705T135106Z-50-5bbb3f4c-1034-4d0c-89dd-50b15e6dc243.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T135546Z-20230705T135106Z-200-569cb372-0408-4fff-beb4-33048ac9d4df.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T124005Z-20230705T113025Z-10050-d29a35cc-2526-4c2b-aa24-91026a46d23c.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T112905Z-20230705T112425Z-200-cca7aad5-1313-49cf-af2b-e15a885e62ad.parquet',
    'rewarded_decisions/appconfig/parquet/2023/07/05/20230705T140527Z-20230705T135706Z-10050-9ab23c97-5886-4f26-bf4f-1c3bf0027da4.parquet'
]


def test_assert_no_overlapping_keys_does_not_raise_for_no_overlapping_keys():
    assert_no_overlapping_keys(NO_OVERLAPPING_S3_KEYS)


def test_assert_no_overlapping_keys_raises_for_all_overlapping_keys():
    with raises(AssertionError) as aerr:
        assert_no_overlapping_keys(ALL_OVERLAPPING_S3_KEYS)


def test_assert_no_overlapping_keys_raises_for_2_overlapping_keys():
    with raises(AssertionError) as aerr:
        assert_no_overlapping_keys(TWO_OVERLAPPING_S3_KEYS)
