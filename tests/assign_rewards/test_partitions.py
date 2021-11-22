# Built-in imports
import string
import io
import os
import gzip
from io import StringIO, BytesIO
import orjson

# External imports
import pandas
import pytest
from pytest_cases import parametrize_with_cases
from pytest_cases import parametrize
from pandas._testing import assert_frame_equal

# Local imports
import config
from firehose_record import FirehoseRecordGroup
from firehose_record import MESSAGE_ID_KEY
from rewarded_decisions import RewardedDecisionPartition


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

def upload_rdrs_as_parquet_files_to_train_bucket(rdrs, model_name):
    """ """

    assert isinstance(model_name, str)

    # Create df based on the rdrs
    df = FirehoseRecordGroup._to_pandas_df(rdrs)
    RDP = RewardedDecisionPartition(model_name=model_name, df=df)
    RDP.sort()
    RDP.save()


def test_partitions_from_firehose_record_group(s3, mocker, get_decision_rec, get_reward_rec, get_rewarded_decision_rec, helpers):
    """
    - Put some records in the Firehose bucket
    - Put some RDRs in the Train bucket
    - Load up a FirehoseRecordGroup and pass it to partitions_from_firehose_record_group()
    - Assert that the list of returned partitions from partitions_from_firehose_record_group
      contain a DataFrame based on the records from the Firehose and Train buckets.
    """

    MODEL_NAME = "test-model-name-1.0"


    ##########################################################################
    # Prepare S3 mock bucket
    ##########################################################################

    # Replace the S3 client used in the app with a mock
    mocker.patch('config.s3client', new=s3)
    
    # Create buckets in the Moto's virtual AWS account
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)
    

    ##########################################################################
    # Generate and add synthetic records to the Firehose bucket
    ##########################################################################

    decision_records = [
        get_decision_rec('111111111000000000000000000'),
        get_decision_rec('111111112000000000000000000'),
        get_decision_rec('111111113000000000000000000')
    ]

    reward_records = [
        get_reward_rec(msg_id_val='111111111000000000000000001', decision_id_val='111111111000000000000000000', reward_val=1),
        get_reward_rec(msg_id_val='111111111000000000000000002', decision_id_val='111111111000000000000000000', reward_val=1),
        
        get_reward_rec(msg_id_val='111111111000000000000000003', decision_id_val='111111112000000000000000000', reward_val=1),
        get_reward_rec(msg_id_val='111111111000000000000000004', decision_id_val='111111112000000000000000000', reward_val=1),
        
        get_reward_rec(msg_id_val='111111111000000000000000005', decision_id_val='111111113000000000000000000', reward_val=1),
        get_reward_rec(msg_id_val='111111111000000000000000006', decision_id_val='111111113000000000000000000', reward_val=1),
    ]
    
    upload_gzipped_jsonl_records_to_firehose_bucket(s3, decision_records+reward_records)


    ##########################################################################
    # Generate and add Rewarded Decision Records to the Train bucket
    ##########################################################################

    rdrs = [
        get_rewarded_decision_rec(decision_id='111111111000000000000000000'),
        get_rewarded_decision_rec(decision_id='111111112000000000000000000')
    ]

    upload_rdrs_as_parquet_files_to_train_bucket(rdrs, model_name=MODEL_NAME)


    ##########################################################################
    # Assertion of the result of partitions_from_firehose_record_group
    # BEFORE executing .load() which retrieves partitions from S3.
    #
    # Generate a DataFrame of Rewarded Decision Records based on the Firehose 
    # S3 records. Used to assert that the partitions returned by
    # partitions_from_firehose_record_group contain a DF of RDR based on the 
    # Firehose records.
    ##########################################################################

    # One FirehoseRecordGroup per model
    firehose_record_groups = FirehoseRecordGroup.load_groups(config.INCOMING_FIREHOSE_S3_KEY)
    assert len(firehose_record_groups) == 1

    expected_rdrs_before_load = []
    for fhr in firehose_record_groups[0].records:
        append = False
        if fhr.is_decision_record() and fhr.message_id <= "111111112000000000000000000":
            append = True
        elif fhr.is_reward_record() and fhr.decision_id <= "111111112000000000000000000":
            append = True

        if append:
            rdr = fhr.to_rewarded_decision_dict()
            expected_rdrs_before_load.append(rdr)
    
    expected_rdrs_before_load.sort(key=lambda rdr: rdr['decision_id'])
    expected_df_before_load = FirehoseRecordGroup._to_pandas_df(expected_rdrs_before_load)

    rewarded_decision_partitions = RewardedDecisionPartition.partitions_from_firehose_record_group(firehose_record_groups[0])
    rdp = rewarded_decision_partitions[0]
    assert_frame_equal(rdp.df, expected_df_before_load, check_column_type=True)


    ##########################################################################
    # Assertion of the result of partitions_from_firehose_record_group
    # AFTER executing .load() which retrieves partitions from S3.
    #
    # The expected DataFrame has RDRs from the Firehose bucket and from the
    # Train bucket.
    ##########################################################################

    expected_rdrs_after_load = list(expected_rdrs_before_load)
    expected_rdrs_after_load.extend(rdrs)
    expected_rdrs_after_load.sort(key=lambda rdr: rdr['decision_id'])
    expected_df_after_load = FirehoseRecordGroup._to_pandas_df(expected_rdrs_after_load)

    # Assert after the S3 Train RDRs have been loaded
    rdp.load()
    rdp.sort()

    assert_frame_equal(rdp.df, expected_df_after_load, check_column_type=True, check_exact=True)
