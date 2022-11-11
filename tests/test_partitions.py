from firehose_record import DF_SCHEMA, FirehoseRecordGroup, DECISION_ID_KEY, MESSAGE_ID_KEY
from partition import RewardedDecisionPartition, parquet_s3_key_prefix
from tests_utils import dicts_to_df


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
