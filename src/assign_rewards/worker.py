import sys
import signal
import concurrent.futures

import join_rewards
import config
import utils

SIGTERM = False


# execute the core worker loop of processing incoming firehose files then 
# processing incoming history files
def worker():
    print(f'starting reward assignment job on node {config.NODE_ID} ({config.NODE_COUNT} nodes total)')

    # identify the portion of incoming history files to process in this node
    incoming_history_files = utils.select_incoming_history_files()

    # group the incoming files by their hashed history ids
    grouped_incoming_history_files = utils.group_files_by_hashed_history_id(incoming_history_files)

    print(f'processing {len(grouped_incoming_history_files)} histories across {len(incoming_history_files)} incoming history files')

    # process each group, perform reward assignment, and upload rewarded decisions to s3
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_incoming_history_file_group, grouped_incoming_history_files)) # list() forces evaluation of generator

    print(f'uploaded {config.stats.rewarded_decision_count} rewarded decision records to {config.TRAIN_BUCKET}')
    print(config.stats)
    print(f'finished reward assignment job node {config.NODE_ID}')


def process_incoming_history_file_group(file_group):
    handle_signals()

    # get the hashed history id
    hashed_history_id = utils.hashed_history_id_from_file(file_group[0])
    
    # add any previously saved history files for this hashed history id
    file_group.extend(utils.history_files_for_hashed_history_id(hashed_history_id))

    # load all records
    records = utils.load_history(file_group)

    # write the consolidated records to a new history file
    utils.save_history(hashed_history_id, records)
    
    # perform validation after consolidation so that invalid records are retained
    # this ensures that any bugs in custom validation code doesn't cause records to be lost
    records = join_rewards.filter_valid_records(hashed_history_id, records)
    
    config.stats.incrementValidatedRecordCount(len(records))

    # assign rewards to decision records.
    rewarded_decisions_by_model = \
        join_rewards.assign_rewards_to_decisions(records)
    
    # upload the updated rewarded decision records to S3
    for model, rewarded_decisions in rewarded_decisions_by_model.items():
        utils.upload_rewarded_decisions(
            model, hashed_history_id, rewarded_decisions)
        config.stats.addModel(model)
        config.stats.incrementRewardedDecisionCount(len(rewarded_decisions))
    
    # delete the incoming and history files that were processed
    utils.delete_all(file_group)


def handle_signals():
    if SIGTERM:
        print(f'Quitting due to SIGTERM signal (node {config.NODE_ID}).')
        sys.exit() # raises SystemExit, so worker threads should have a chance to finish up


def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f"SIGTERM received (node {config.NODE_ID}).")
    return


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
