# Built-in imports
import operator
import uuid
import random
import datetime
from datetime import timedelta
from datetime import timezone

# External imports
import pandas as pd
from pandas._testing import assert_frame_equal
from ksuid import Ksuid

# Local imports
import config
import rewarded_decisions
import utils
import firehose_record
from firehose_record import DF_SCHEMA, FirehoseRecordGroup, DECISION_ID_KEY, MESSAGE_ID_KEY
from rewarded_decisions import RewardedDecisionPartition, s3_key_prefix, repair_overlapping_keys
from ingest_firehose_tests.utils import dicts_to_df
from ingest_firehose_tests.utils import upload_gzipped_jsonl_records_to_firehose_bucket


def upload_rdrs_as_parquet_files_to_train_bucket(rdrs, model_name):
    """ """

    assert isinstance(model_name, str)

    # Create df based on the rdrs
    df = dicts_to_df(dicts=rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
    RDP = RewardedDecisionPartition(model_name=model_name, df=df)
    RDP.sort()
    RDP.save()


def get_rdrs_before_or_after_decision_id(
    firehose_record_group,
    pivot1,
    op1,
    pivot2=None,
    op2=None):
    """
    Convert records from a Firehose Record Group into RDRs and return a subset
    of them depending if they are "<=" or ">" than a pivot record.
    """
    
    expected_rdrs = []
    for fhr in firehose_record_group.records:
        
        rdr = fhr.to_rewarded_decision_dict()

        if fhr.is_decision_record():
            if op1(fhr.message_id, pivot1):
                if op2 is None and pivot2 is None:
                    expected_rdrs.append(rdr)
                else:
                    if op2(fhr.message_id, pivot2):
                        expected_rdrs.append(rdr)

        elif fhr.is_reward_record():
            if op1(fhr.decision_id, pivot1):
                if op2 is None and pivot2 is None:
                    expected_rdrs.append(rdr)
                else:
                    if op2(fhr.decision_id, pivot2):
                        expected_rdrs.append(rdr)
                    else:
                        print(f"Passing from: {fhr.message_id}")
    
    expected_rdrs.sort(key=lambda rdr: rdr[DECISION_ID_KEY])
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

    # Create buckets in the Moto's virtual AWS account
    s3.create_bucket(Bucket=config.FIREHOSE_BUCKET)
    s3.create_bucket(Bucket=config.TRAIN_BUCKET)

    ##########################################################################
    # Generate and add synthetic records to the Firehose bucket
    ##########################################################################

    FIRST_DECISION_ID_IN_TRAIN_BUCKET = '100011111000000000000000000'
    LAST_DECISION_ID_IN_TRAIN_BUCKET = '111111112000000000000000000'
    DECISION_ID_NOT_IN_TRAIN_BUCKET = '111119000000000000000000000'

    decision_records = [
        get_decision_rec(FIRST_DECISION_ID_IN_TRAIN_BUCKET), # In Train bucket 
        get_decision_rec('111111111000000000000000000'),     # In Train bucket
        get_decision_rec(LAST_DECISION_ID_IN_TRAIN_BUCKET),  # In Train bucket
        get_decision_rec(DECISION_ID_NOT_IN_TRAIN_BUCKET)    # NOT in Train bucket
    ]

    reward_records = [
        get_reward_rec(decision_id_val=FIRST_DECISION_ID_IN_TRAIN_BUCKET), # In Train bucket

        get_reward_rec(decision_id_val='111111111000000000000000000'), # In Train bucket
        get_reward_rec(decision_id_val='111111111000000000000000000'), # In Train bucket
        
        get_reward_rec(decision_id_val=LAST_DECISION_ID_IN_TRAIN_BUCKET), # In Train bucket
        get_reward_rec(decision_id_val=LAST_DECISION_ID_IN_TRAIN_BUCKET), # In Train bucket
        
        get_reward_rec(decision_id_val=DECISION_ID_NOT_IN_TRAIN_BUCKET), # NOT in Train bucket
        get_reward_rec(decision_id_val=DECISION_ID_NOT_IN_TRAIN_BUCKET), # NOT in Train bucket
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

    # Parquet file with a single record
    rdrs0 = [
        get_rewarded_decision_rec(decision_id=FIRST_DECISION_ID_IN_TRAIN_BUCKET),
    ]
    upload_rdrs_as_parquet_files_to_train_bucket(rdrs0, model_name=MODEL_NAME)


    rdrs1 = [
        get_rewarded_decision_rec(decision_id='111111111000000000000000000'),
        get_rewarded_decision_rec(decision_id=LAST_DECISION_ID_IN_TRAIN_BUCKET),
    ]
    upload_rdrs_as_parquet_files_to_train_bucket(rdrs1, model_name=MODEL_NAME)

    
    # To be sure that I know the max decision_id in the Train bucket
    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{MODEL_NAME}' )
    keys_in_bucket = [s3file['Key'] for s3file in response['Contents']]
    assert s3_key_prefix(MODEL_NAME, LAST_DECISION_ID_IN_TRAIN_BUCKET) in max(keys_in_bucket)


    ##########################################################################
    # Assert the generated partitions BEFORE executing .load(), which 
    # retrieves partitions from S3.
    ##########################################################################

    firehose_record_groups = \
        FirehoseRecordGroup.load_groups(config.INCOMING_FIREHOSE_S3_KEY)
    
    # One FirehoseRecordGroup per model
    assert len(firehose_record_groups) == 1
    firehose_record_group = firehose_record_groups[0]

    rdps = RewardedDecisionPartition.partitions_from_firehose_record_group(
        firehose_record_group)


    #
    # First partition
    #
    # Expected DataFrame of RDRs based on a Firehose S3 record
    # that DO has a corresponding decision_id in the Train bucket.
    expected_df_before_load0 = get_rdrs_before_or_after_decision_id(
        firehose_record_group = firehose_record_group,
        pivot1 = FIRST_DECISION_ID_IN_TRAIN_BUCKET,
        op1 = operator.le)
    checked_df = rdps[0].df.sort_values(['decision_id', 'timestamp']).reset_index(drop=True)
    assert_frame_equal(checked_df, expected_df_before_load0, check_column_type=True)
    assert_only_last_partition_may_not_have_an_s3_key(rdps)


    #
    # Second partition
    #
    # Expected DataFrame of RDRs based on Firehose S3 records 
    # that DO have a corresponding decision_id in the Train bucket.
    expected_df_before_load1 = get_rdrs_before_or_after_decision_id(
        firehose_record_group = firehose_record_group,
        pivot1 = LAST_DECISION_ID_IN_TRAIN_BUCKET,
        op1 = operator.le,
        pivot2 = FIRST_DECISION_ID_IN_TRAIN_BUCKET,
        op2 = operator.gt)
    checked_df = rdps[1].df.sort_values(['decision_id', 'timestamp']).reset_index(drop=True)
    assert_frame_equal(checked_df, expected_df_before_load1, check_column_type=True)
    assert_only_last_partition_may_not_have_an_s3_key(rdps)


    #
    # Third partition
    #
    # Expected DataFrame of RDRs based on the Firehose S3 records 
    # that DON'T have a corresponding decision_id in the Train bucket.
    expected_df_before_load2 = get_rdrs_before_or_after_decision_id(
        firehose_record_group = firehose_record_group,
        pivot1 = LAST_DECISION_ID_IN_TRAIN_BUCKET,
        op1 = operator.gt)
    checked_df = rdps[2].df.sort_values(['decision_id', 'timestamp']).reset_index(drop=True)
    assert_frame_equal(checked_df, expected_df_before_load2, check_column_type=True)
    assert_only_last_partition_may_not_have_an_s3_key(rdps)


    ##########################################################################
    # Assert the generated partitions AFTER executing .load(), which 
    # retrieves partitions from S3. The expected DataFrame has RDRs from the 
    # Firehose bucket and from the Train bucket.
    ##########################################################################

    expected_df_after_load0 = pd.concat(
        [expected_df_before_load0, dicts_to_df(dicts=rdrs0, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)],
        ignore_index=True).sort_values(DECISION_ID_KEY, ignore_index=True)

    # Assert after the S3 Train RDRs have been loaded
    rdps[0].load()
    rdps[0].sort()

    assert_frame_equal(
        rdps[0].df, expected_df_after_load0,
        check_column_type=True, check_exact=True, check_index_type=True)


    expected_df_after_load1 = pd.concat(
        [expected_df_before_load1, dicts_to_df(dicts=rdrs1, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)],
        ignore_index=True).sort_values(DECISION_ID_KEY, ignore_index=True)

    # Assert after the S3 Train RDRs have been loaded
    rdps[1].load()
    rdps[1].sort()

    assert_frame_equal(
        rdps[1].df, expected_df_after_load1,
        check_column_type=True, check_exact=True, check_index_type=True)


    ##########################################################################
    # Assert the presence of new created Parquet files in Train bucket due to 
    # no partitions being found for certain Firehose records.
    ##########################################################################

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{MODEL_NAME}')
    s3_keys_in_train_bucket_before_clean = [s3file['Key'] for s3file in response['Contents']]

    # Number of initial keys in bucket
    assert len(s3_keys_in_train_bucket_before_clean) == 2

    for rdp in rdps:
        rdp.process()

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{MODEL_NAME}')
    s3_keys_in_train_bucket_after_clean = [s3file['Key'] for s3file in response['Contents']]

    # Assert presence of a new Parquet file due to new Firehose records
    expected_key_prefix = s3_key_prefix(MODEL_NAME, DECISION_ID_NOT_IN_TRAIN_BUCKET)
    assert any(map(lambda x: expected_key_prefix in x, s3_keys_in_train_bucket_after_clean))

    # Number of final keys in bucket
    assert len(s3_keys_in_train_bucket_after_clean) == 3

    # Old keys deleted
    for key in s3_keys_in_train_bucket_before_clean:
        assert key not in s3_keys_in_train_bucket_after_clean 
   

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
    # eb1167b3-67a9-4378-bc65-c1e582e2e662 <- 3rd
    # f7c1bd87-4da5-4709-9471-3d60c8a70639 <- 4th
    # e443df78-9558-467f-9ba9-1faf7a024204 <- 5th
    # 23a7711a-8133-4876-b7eb-dcd9e87a1613 <- 6th
    # 1846d424-c17c-4279-a3c6-612f48268673 <- 7th
    # fcbd04c3-4021-4ef7-8ca5-a5a19e4d6e3c <- 8th
    # b4862b21-fb97-4435-8856-1712e8e5216a <- 9th
    # 259f4329-e6f4-490b-9a16-4106cf6a659e <- 10th
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
    # 0123456789 ...  20
    # ||||||    
    #   |||     
    #     |||   
    #         ||
    #                 |

    # Output
    # 0123456789 ... 20
    # |||||||
    #         ||
    #                |

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

    rdrs5 = [
        get_rewarded_decision_rec(decision_id=str(Ksuid(dt_zero+timedelta(days=20), payload))),
    ]

    ##########################################################################
    # Upload the RDRs as parquet files to the mocked S3 bucket
    ##########################################################################
    
    rdrs_list = [rdrs1, rdrs2, rdrs3, rdrs4, rdrs5]
    RDPs = []

    for rdrs in rdrs_list:
        df = dicts_to_df(dicts=rdrs, columns=DF_SCHEMA.keys(), dtypes=DF_SCHEMA)
        RDP = RewardedDecisionPartition(model_name=MODEL_NAME, df=df)
        RDP.sort()
        RDP.save()
        RDPs.append(RDP)


    ##########################################################################
    # Assertion original keys are present before fix
    ##########################################################################

    original_keys = [
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/05/20210105T000000Z-20210103T000000Z-f728b4fa-4248-4e3a-8a5d-2f346baa9455.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/07/20210107T000000Z-20210105T000000Z-eb1167b3-67a9-4378-bc65-c1e582e2e662.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/10/20210110T000000Z-20210109T000000Z-f7c1bd87-4da5-4709-9471-3d60c8a70639.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/21/20210121T000000Z-20210121T000000Z-e443df78-9558-467f-9ba9-1faf7a024204.parquet"
    ]

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{MODEL_NAME}')
    keys_in_bucket = [x['Key'] for x in response['Contents']]

    for i in original_keys:
        assert i in keys_in_bucket, f"{i} not in bucket"


    ##########################################################################
    # Repair
    ##########################################################################

    repair_overlapping_keys(MODEL_NAME, RDPs)


    ##########################################################################
    # List the actual keys after repair
    ##########################################################################

    response = s3.list_objects_v2(
        Bucket = config.TRAIN_BUCKET,
        Prefix = f'rewarded_decisions/{MODEL_NAME}')

    keys_in_bucket = [x['Key'] for x in response['Contents']]


    ##########################################################################
    # Assert old keys deleted
    ##########################################################################

    for i in original_keys[:3]: assert i not in keys_in_bucket


    ##########################################################################
    # Assertions after repair
    ##########################################################################

    expected_keys = [
        {
            "key" : "rewarded_decisions/test-model-name-1.0/parquet/2021/01/07/20210107T000000Z-20210101T000000Z-23a7711a-8133-4876-b7eb-dcd9e87a1613.parquet",
            "n_records" : 7
        },
        {
            "key" : "rewarded_decisions/test-model-name-1.0/parquet/2021/01/10/20210110T000000Z-20210109T000000Z-f7c1bd87-4da5-4709-9471-3d60c8a70639.parquet",
            "n_records" : 2
        },
        {
            "key" : "rewarded_decisions/test-model-name-1.0/parquet/2021/01/21/20210121T000000Z-20210121T000000Z-e443df78-9558-467f-9ba9-1faf7a024204.parquet",
            "n_records" : 1
        }
    ]
    
    # Assertion of number of keys after repair
    assert len(keys_in_bucket) == len(expected_keys), "Too many keys in bucket, were the old ones deleted?"

    
    for expected in expected_keys:
        expected_key = expected["key"]
        expected_n_records = expected["n_records"]
        
        # Assert presence of the new generated s3 keys due to new Firehose records
        assert expected_key in keys_in_bucket, f"Expected:\n{expected_key}"

        # Assert the contents
        df = pd.read_parquet(f's3://{config.TRAIN_BUCKET}/{expected_key}')
        assert df.shape[0] == expected_n_records
