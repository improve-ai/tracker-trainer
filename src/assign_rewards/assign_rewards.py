from history_record import HistoryRecord

def assign_rewards(history_records: list[HistoryRecord]):
    """
    Assign rewards to decision records.
    
    Records may be mutated. Mutations will not be persisted to the history
    storage in EFS.
    
    Exceptions raised here will cause the reward assignment job to abort.

    Args:
        history_records: a list of HistoryRecord obects, sorted by 'timestamp'
        and in the case of a tied timestamp, secondarily sorted with 
        records of 'type' == 'decision' prior to other record types.
    """
    for i in range(len(history_records)):
        record = history_records[i]
        if record.is_decision_record():
            assign_rewards_to(record, (history_records[j] for j in range(i+1, len(history_records))))

    
def assign_rewards_to(assignee: dict, remaining_history: Iterator[HistoryRecord]):
    for record in remaining_history:
        if not assignee.reward_window_contains(record):
            return
        
        assignee.reward += record.value
