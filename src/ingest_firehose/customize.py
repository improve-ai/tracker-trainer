# 
# Perform customization of a record on load from the firehose S3 bucket.
# 
# parameters:
#  - record - the loaded record to modify
# returns:
#  - the customized record or None to skip the record
#
# customize_record() is called immediately after record load from the Firehose S3, before validation, and before saving the record to the histories EFS.
# Records that fail validation will be skipped and not saved to EFS. Exceptions raised here will cause the job to fail.
#
# NOTE: It is generally preferable to perform record/history/reward customization in assign_rewards/customize.customize_history(), not here.
# Record customization performed here on ingest is persisted to EFS, while customization performed in assign_rewards/customize.customize_history()
# only affects the final rewarded decision records uploaded to the train S3 bucket. When mistakes are made, it is generally easier to clean up the train S3 bucket
# than it is to clean up the EFS volume.
#
def customize_record(record: dict):
    return record
