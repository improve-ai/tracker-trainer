def before_assign_rewards(history_records: list):
    """
    Implement record customization that occurs before reward assignment here.
    
    Records may be mutated. Mutations will not be persisted to the history
    storage in EFS.
    
    Exceptions raised here will cause the reward assignment job to abort.
    
    Examples of the types of customization to perform here:
        - custom event values
        - reward bonuses for things like user retention or engagement
    
    Args:
        history_records: a list of record dicts, sorted by 'timestamp'
        and in the case of a tied timestamp, secondarily sorted with 
        records of 'type' == 'decision' prior to other record types.
        'timestamp' is guaranteed to be present on each record and is of 
        type datetime.datetime.
    
    Returns:
        the list of records to perform reward assignment on
    """
    return history_records
    
def after_assign_rewards(history_records: list):
    """
    Implement record customization that occurs before uploading rewarded
    decisions to the train S3 bucket here.  If you wish to completely
    customize the reward assignement process, this is the place to do it.
    
    Records may be mutated. Mutations will not be persisted to the history
    storage in EFS.
    
    Exceptions raised here will cause the reward assignment job to abort.
    
    Args:
        history_records: a list of record dicts, sorted by timestamp
        and secondarily sorted with records of 'type' == 'decision' prior
        to other record types.  'timestamp' is guaranteed to be present
        on each record and is of type datetime.datetime. records of 
        'type' == 'decision' will already have had their 'reward' value set 
        during assign_rewards.
    
    Returns:
        the list of records, containing, at a minumum, the rewarded decision
        records to upload to the S3 "train" bucket. records of 'type' != 'decision'
        will be ignored during further processing so may optionally be ommitted
        from the return value.
    """
    return history_records