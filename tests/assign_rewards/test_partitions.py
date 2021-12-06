# Built-in imports
import gzip
from io import BytesIO
import operator
import uuid
import random
import datetime
from datetime import timedelta
from datetime import timezone

# External imports
import pandas as pd
import orjson
from pandas._testing import assert_frame_equal
from ksuid import Ksuid

# Local imports
import config
import rewarded_decisions
import utils
import firehose_record
from firehose_record import DF_SCHEMA, FirehoseRecordGroup, DECISION_ID_KEY, MESSAGE_ID_KEY
from rewarded_decisions import RewardedDecisionPartition, s3_key_prefix, repair_overlapping_keys
from tests.ingest_firehose.utils import dicts_to_df


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
    df = dicts_to_df(dicts=rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
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
    # df = FirehoseRecordGroup._to_pandas_df(expected_rdrs)
    df = dicts_to_df(dicts=expected_rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)

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
    # mocker.patch('config.s3client', new=s3)
    utils.s3client = rewarded_decisions.s3client = firehose_record.s3client = s3
    # firehose_record.s3client = rewarded_decisions.s3client = \
    #     src.ingest_firehose.utils = s3

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
        get_decision_rec('111119000000000000000000000')  # NOT in Train bucket
    ]

    reward_records = [
        get_reward_rec(decision_id_val='111111111000000000000000000'), # In Train bucket
        get_reward_rec(decision_id_val='111111111000000000000000000'), # In Train bucket
        
        get_reward_rec(decision_id_val='111111112000000000000000000'), # In Train bucket
        get_reward_rec(decision_id_val='111111112000000000000000000'), # In Train bucket
        
        get_reward_rec(decision_id_val='111119000000000000000000000'), # NOT in Train bucket
        get_reward_rec(decision_id_val='111119000000000000000000000'), # NOT in Train bucket
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

    # TODO this is just a trick to make sorting equal in both expected and
    #  calculated data
    checked_df = rdps[0].df.sort_values(['decision_id', 'timestamp']).reset_index(drop=True)

    assert_frame_equal(checked_df, expected_df_before_load1, check_column_type=True)
    assert_only_last_partition_may_not_have_an_s3_key(rdps)


    # Generate an expected DataFrame of RDRs based on the Firehose S3 records 
    # that DON'T have a corresponding decision_id in the Train bucket.
    expected_df_before_load2 = get_rdrs_before_or_after_decision_id(
        firehose_record_group = firehose_record_group,
        pivot = LAST_DECISION_ID_IN_TRAIN_BUCKET,
        op = operator.gt)

    # config.s3client = firehose_record.s3client = rewarded_decisions.s3client = utils.s3_client = s3
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
        [expected_df_before_load1, dicts_to_df(dicts=rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)],
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
    

def test_repair_overlapping_keys(s3, mocker, get_rewarded_decision_rec):

    MODEL_NAME = "test-model-name-1.0"

    # Replace the used s3 client with a mocked one 
    utils.s3client = rewarded_decisions.s3client = s3

    # Create a mocked bucket
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)

    rd = random.Random()
    rd.seed(0)

    # Patch the uuid4 function with one which uses seeded random bits
    # to always get known uuid4s:
    # e3e70682-c209-4cac-a29f-6fbed82c07cd <- first uuid generated by seeded uuid4()
    # f728b4fa-4248-4e3a-8a5d-2f346baa9455 <- second uuid generated by seeded uuid4()
    # eb1167b3-67a9-4378-bc65-c1e582e2e662 <- ...
    # f7c1bd87-4da5-4709-9471-3d60c8a70639
    # e443df78-9558-467f-9ba9-1faf7a024204
    # 23a7711a-8133-4876-b7eb-dcd9e87a1613
    # 1846d424-c17c-4279-a3c6-612f48268673
    # fcbd04c3-4021-4ef7-8ca5-a5a19e4d6e3c
    # b4862b21-fb97-4435-8856-1712e8e5216a
    # 259f4329-e6f4-490b-9a16-4106cf6a659e
    mocker.patch('rewarded_decisions.uuid4', new=lambda: uuid.UUID(int=rd.getrandbits(128), version=4))


    ##########################################################################
    # Create synthetic Rewarded Decision Records.
    # The decision_ids of the RDRs will be created based on known dates 
    # (and a known payload).
    # The intention is to create two sets of RDRs with some overlapping RDRs
    ##########################################################################
    
    dt_zero = datetime.datetime(year=2021, month=1, day=1, tzinfo=timezone.utc)
    payload = b'0000000000000000'
    
    # Input
    # 0123456789
    # ||||||    
    #   |||     
    #     |||   
    #         ||

    # Output
    # 0123456789
    # |||||||
    #         ||

    rdrs1 = [
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=0), payload))),
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=1), payload))),
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=2), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=3), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=4), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=5), payload))), # overlapping
    ]

    rdrs2 = [
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=2), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=3), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=4), payload))), # overlapping
    ]

    rdrs3 = [
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=4), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=5), payload))), # overlapping
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=6), payload)))
    ]

    rdrs4 = [
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=8), payload))),
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=9), payload))),
    ]

    ##########################################################################
    # Upload the RDRs as parquet files to the mocked S3 bucket
    ##########################################################################
    
    # The record uploaded by .save() is called:
    # /rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet
    df1 = dicts_to_df(dicts=rdrs1, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    RDP1 = RewardedDecisionPartition(model_name=MODEL_NAME, df=df1)
    RDP1.sort()
    RDP1.save()

    # /rewarded_decisions/test-model-name-1.0/parquet/2021/01/05/20210105T000000Z-20210103T000000Z-f728b4fa-4248-4e3a-8a5d-2f346baa9455.parquet
    df2 = dicts_to_df(dicts=rdrs2, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    RDP2 = RewardedDecisionPartition(model_name=MODEL_NAME, df=df2)
    RDP2.sort()
    RDP2.save()

    # /rewarded_decisions/test-model-name-1.0/parquet/2021/01/07/20210107T000000Z-20210105T000000Z-eb1167b3-67a9-4378-bc65-c1e582e2e662.parquet
    df3 = dicts_to_df(dicts=rdrs3, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    RDP3 = RewardedDecisionPartition(model_name=MODEL_NAME, df=df3)
    RDP3.sort()
    RDP3.save()

    # /rewarded_decisions/test-model-name-1.0/parquet/2021/01/10/20210110T000000Z-20210109T000000Z-f7c1bd87-4da5-4709-9471-3d60c8a70639.parquet
    df4 = dicts_to_df(dicts=rdrs4, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    RDP4 = RewardedDecisionPartition(model_name=MODEL_NAME, df=df4)
    RDP4.sort()
    RDP4.save()

    ##########################################################################
    # Assertion original keys are present before fix
    ##########################################################################

    original_keys = [
        "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/03/20210103T000000Z-20210101T000000Z-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
        "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/05/20210105T000000Z-20210103T000000Z-f728b4fa-4248-4e3a-8a5d-2f346baa9455.parquet",
        "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/07/20210107T000000Z-20210105T000000Z-eb1167b3-67a9-4378-bc65-c1e582e2e662.parquet",
        "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/10/20210110T000000Z-20210109T000000Z-f7c1bd87-4da5-4709-9471-3d60c8a70639.parquet"
    ]
    

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'/rewarded_decisions/{MODEL_NAME}')
    keys_in_bucket = [x['Key'] for x in response['Contents']]

    for i in original_keys:
        assert i in keys_in_bucket, f"{i} not in bucket"


    ##########################################################################
    # Repair
    ##########################################################################

    RDPs = [RDP1, RDP2, RDP3, RDP4]
    repair_overlapping_keys(MODEL_NAME, RDPs)


    ##########################################################################
    # Assertion of number of keys after repair
    ##########################################################################

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'/rewarded_decisions/{MODEL_NAME}')
    keys_in_bucket = [x['Key'] for x in response['Contents']]

    assert len(keys_in_bucket) == 2, "Too many keys in bucket, were the old ones deleted?"


    ##########################################################################
    # Assert old keys deleted
    ##########################################################################

    for i in original_keys[:3]: assert i not in keys_in_bucket


    ##########################################################################
    # Assertion of actual keys after repair
    ##########################################################################

    # Assert presence of the new generated s3 keys due to new Firehose records
    expected_key1 = "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/07/20210107T000000Z-20210101T000000Z-e443df78-9558-467f-9ba9-1faf7a024204.parquet"
    expected_key2 = "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/10/20210110T000000Z-20210109T000000Z-f7c1bd87-4da5-4709-9471-3d60c8a70639.parquet"
    assert expected_key1 in keys_in_bucket, f"Expected:\n{expected_key1}"
    assert expected_key2 in keys_in_bucket, f"Expected:\n{expected_key2}"


    ##########################################################################
    # Assert the contents
    ##########################################################################

    s3_df1 = pd.read_parquet(f's3://{config.TRAIN_BUCKET}/{expected_key1}')
    assert s3_df1.shape[0] == 7
    
    s3_df2 = pd.read_parquet(f's3://{config.TRAIN_BUCKET}/{expected_key2}')
    assert s3_df2.shape[0] == 2




