# Built-in imports
import gzip
from io import BytesIO
import orjson
import operator

# External imports
import pandas as pd
from pandas._testing import assert_frame_equal

# Local imports
import config
from firehose_record import FirehoseRecordGroup
from firehose_record import DECISION_ID_KEY
from firehose_record import MESSAGE_ID_KEY
from rewarded_decisions import RewardedDecisionPartition
from rewarded_decisions import s3_key_prefix


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


def get_rdrs_before_or_after_decision_id(firehose_record_group, pivot, op):
    """
    Convert records from a Firehose Record Group into RDRs and return a subset
    of them depending if they are "<=" or ">" than a pivot record.
    """
    
    expected_rdrs = []
    for fhr in firehose_record_group.records:
        
        rdr = fhr.to_rewarded_decision_dict()

        if fhr.is_decision_record():
            if op(fhr.message_id, pivot):
                expected_rdrs.append(rdr)

        elif fhr.is_reward_record():
            if op(fhr.decision_id, pivot):
                expected_rdrs.append(rdr)
    
    expected_rdrs.sort(key=lambda rdr: rdr[DECISION_ID_KEY])
    df = FirehoseRecordGroup._to_pandas_df(expected_rdrs)

    return df


def assert_only_last_partition_may_not_have_an_s3_key(rdps):
    if len(rdps) > 1:
        assert all(rdp.s3_key is not None for rdp in rdps[:-1])


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

    # The maximum value of all the decision_ids in the Train bucket
    LAST_DECISION_ID_IN_TRAIN_BUCKET = '111111112000000000000000000'

    decision_records = [
        get_decision_rec('111111111000000000000000000'), # In Train bucket
        get_decision_rec('111111112000000000000000000'), # In Train bucket 
        get_decision_rec('111111113000000000000000000')  # NOT in Train bucket
    ]

    reward_records = [
        get_reward_rec(decision_id_val='111111111000000000000000000'), # In Train bucket
        get_reward_rec(decision_id_val='111111111000000000000000000'), # In Train bucket
        
        get_reward_rec(decision_id_val='111111112000000000000000000'), # In Train bucket
        get_reward_rec(decision_id_val='111111112000000000000000000'), # In Train bucket
        
        get_reward_rec(decision_id_val='111111113000000000000000000'), # NOT in Train bucket
        get_reward_rec(decision_id_val='111111113000000000000000000'), # NOT in Train bucket
    ]

    # Assert that there is at least one decision_id with the value of 
    # LAST_DECISION_ID_IN_TRAIN_BUCKET both in the decision records
    # and in the reward records
    assert any(dr[MESSAGE_ID_KEY] == LAST_DECISION_ID_IN_TRAIN_BUCKET 
        for dr in decision_records)
    assert any(rr[DECISION_ID_KEY] == LAST_DECISION_ID_IN_TRAIN_BUCKET
        for rr in reward_records)

    upload_gzipped_jsonl_records_to_firehose_bucket(s3, decision_records+reward_records)


    ##########################################################################
    # Generate and add Rewarded Decision Records to the Train bucket
    ##########################################################################

    rdrs = [
        get_rewarded_decision_rec(decision_id='111111111000000000000000000'),
        get_rewarded_decision_rec(decision_id='111111112000000000000000000'),
    ]

    # To be sure that I know the max decision_id in the Train bucket
    assert LAST_DECISION_ID_IN_TRAIN_BUCKET == max(rdr[DECISION_ID_KEY] 
        for rdr in rdrs)

    upload_rdrs_as_parquet_files_to_train_bucket(rdrs, model_name=MODEL_NAME)


    ##########################################################################
    # Assert the generated partitions BEFORE executing .load(), which 
    # retrieves partitions from S3.
    ##########################################################################

    firehose_record_groups = \
        FirehoseRecordGroup.load_groups(config.INCOMING_FIREHOSE_S3_KEY)
    
    # One FirehoseRecordGroup per model
    assert len(firehose_record_groups) == 1
    firehose_record_group = firehose_record_groups[0]

    # Generate an expected DataFrame of RDRs based on the Firehose S3 records 
    # that DO have a corresponding decision_id in the Train bucket.
    expected_df_before_load1 = get_rdrs_before_or_after_decision_id(
        firehose_record_group = firehose_record_group,
        pivot = LAST_DECISION_ID_IN_TRAIN_BUCKET,
        op = operator.le)
    rdps = RewardedDecisionPartition.partitions_from_firehose_record_group(
        firehose_record_group)
    assert_frame_equal(rdps[0].df, expected_df_before_load1, check_column_type=True)
    assert_only_last_partition_may_not_have_an_s3_key(rdps)


    # Generate an expected DataFrame of RDRs based on the Firehose S3 records 
    # that DON'T have a corresponding decision_id in the Train bucket.
    expected_df_before_load2 = get_rdrs_before_or_after_decision_id(
        firehose_record_group = firehose_record_group,
        pivot = LAST_DECISION_ID_IN_TRAIN_BUCKET,
        op = operator.gt)
    rdps = RewardedDecisionPartition.partitions_from_firehose_record_group(
        firehose_record_group)
    assert_frame_equal(rdps[1].df, expected_df_before_load2, check_column_type=True)
    assert_only_last_partition_may_not_have_an_s3_key(rdps)


    ##########################################################################
    # Assert the generated partitions AFTER executing .load(), which 
    # retrieves partitions from S3. The expected DataFrame has RDRs from the 
    # Firehose bucket and from theTrain bucket.
    ##########################################################################

    expected_df_after_load1 = pd.concat(
        [expected_df_before_load1, FirehoseRecordGroup._to_pandas_df(rdrs)],
        ignore_index=True).sort_values(DECISION_ID_KEY, ignore_index=True)

    # Assert after the S3 Train RDRs have been loaded
    rdps[0].load()
    rdps[0].sort()
    assert_frame_equal(
        rdps[0].df, expected_df_after_load1,
        check_column_type=True, check_exact=True, check_index_type=True
    )
    
    
    ##########################################################################
    # Assert the presence of new files due to no partitions being found for 
    # certain Firehose records
    ##########################################################################

    for rdp in rdps:
        rdp.load()
        rdp.sort()
        rdp.merge()
        rdp.save()

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'/rewarded_decisions/{MODEL_NAME}'
    )

    # Assert presence of the new generated s3 key due to new Firehose records
    # TODO: this may need to be modified/updated/improved
    expected_key_prefix = s3_key_prefix(MODEL_NAME, '111111113000000000000000000')
    assert any(map(lambda x: expected_key_prefix in x, [s3file['Key'] for s3file in response['Contents']]))
    

    assert_frame_equal(rdp.df, expected_df_after_load, check_column_type=True, check_exact=True)
