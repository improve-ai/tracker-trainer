# Built-in imports
from pathlib import Path

# External imports
import orjson
import pytest

# Local imports
import config
from config import TRAIN_BUCKET
import utils
from worker import worker
import firehose_record
# from firehose_record import assert_valid_record
from firehose_record import DECISION_ID_KEY
from firehose_record import DF_SCHEMA
import rewarded_decisions
from rewarded_decisions import s3_key
from ingest_firehose_tests.utils import upload_gzipped_jsonl_records_to_firehose_bucket
from ingest_firehose_tests.utils import dicts_to_df


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
    firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3

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
    df.to_parquet(f's3://{TRAIN_BUCKET}/{parquet_key}', engine=ENGINE, compression='ZSTD', index=False)

    # Ensure the key is really there
    response = s3.list_objects_v2(
        Bucket = TRAIN_BUCKET,
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
        worker()


    # Ensure the parquet file is in the unrecoverable path
    unrecoverable_key = f'unrecoverable/{parquet_key}'
    response = s3.list_objects_v2(
        Bucket = TRAIN_BUCKET,
        Prefix = f'unrecoverable/')
    all_keys = [x['Key'] for x in response['Contents']]
    assert unrecoverable_key in all_keys


    # Ensure the original faulty parquet file got deleted
    response = s3.list_objects_v2(
        Bucket = TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{model_name}')
    assert 'Contents' not in response # No 'Content's == no files