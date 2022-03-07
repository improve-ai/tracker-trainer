# Built-in imports
import orjson
from pathlib import Path

# External imports
import orjson
import pandas as pd

# Local imports
import firehose_record
from firehose_record import FirehoseRecord
from firehose_record import assert_valid_rewarded_decision_record
from firehose_record import TYPE_KEY
from firehose_record import DF_SCHEMA
import rewarded_decisions
from rewarded_decisions import RewardedDecisionPartition
from rewarded_decisions import parquet_s3_key
from worker import worker
from tests_utils import upload_gzipped_jsonl_records_to_firehose_bucket
from tests_utils import dicts_to_df
import utils
import config
from config import TRAIN_BUCKET
from config import stats

ENGINE="fastparquet"


def not_test_yet_logs(s3, get_reward_rec, caplog):
    """
    This is not a test (yet).
    This function is useful to see the test logs running.
    It will become a test when the desired logs and syntax becomes final.
    """

    firehose_record.s3client = utils.s3client = rewarded_decisions.s3client = s3
    
    # Create mocked buckets
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)


    ##########################################################################
    # Craft a .parquet file whose decision_id matches the one from
    # the JSONL records
    ##########################################################################
    
    min_decision_id = '23wDoRiHhhjNzC4SEPpOHaNQHhB'
    max_decision_id = '23wDoRiHhhjNzC4SEPpOHaNQHhC'

    record1 = get_reward_rec(decision_id_val=min_decision_id)
    record2 = get_reward_rec(decision_id_val=max_decision_id)

    RDR1 = FirehoseRecord(record1).to_rewarded_decision_dict()
    RDR2 = FirehoseRecord(record2).to_rewarded_decision_dict()
    
    assert_valid_rewarded_decision_record(RDR1, record1[TYPE_KEY])
    assert_valid_rewarded_decision_record(RDR2, record2[TYPE_KEY])


    crafted_df = dicts_to_df(
        dicts=[RDR1, RDR2],
        columns=DF_SCHEMA.keys(),
        dtypes=DF_SCHEMA )

    # WHEN I USE THIS TO CRAFT A PARQUET FILE, STATS INCREMENTS
    RDP = RewardedDecisionPartition("appconfig", crafted_df)
    RDP.sort()
    RDP.merge()
    parquet_key = parquet_s3_key("appconfig", min_decision_id, max_decision_id, RDP.df.shape[0])
    RDP.df.to_parquet(
        f's3://{TRAIN_BUCKET}/{parquet_key}',
        engine=ENGINE,
        compression='ZSTD',
        index=False )


    ##########################################################################
    # Load a real JSONL's data into the firehose bucket
    # Contains records and rewards from various different models
    ##########################################################################

    p = Path("test_cases/data/ingest/logging")
    for jsonl_file in p.glob('**/improveai-acme-demo-kw0-firehose-2-2022-01-19-23-53-50-f6a57d75-bd1b-4328-a87c-2d94ed63e10f'):
        records = []
        with jsonl_file.open() as f:
            for line in f.readlines():
                record = orjson.loads(line)
                records.append(record)
    
        upload_gzipped_jsonl_records_to_firehose_bucket(s3, records)

    stats.reset()
    worker()
