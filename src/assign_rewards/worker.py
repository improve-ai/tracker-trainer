import sys
import signal
import concurrent.futures

import config
import utils
import history
from history import History

SIGTERM = False

# execute the core worker loop of processing incoming firehose files then
# processing incoming history files
def worker():
    print(f'starting reward assignment job on node {config.NODE_ID} ({config.NODE_COUNT} nodes total)')

    # identify the portion of incoming history files to process in this node
    incoming_history_files = history.select_incoming_history_files()

    # group the incoming files by their hashed history ids
    grouped_incoming_history_files = history.group_files_by_hashed_history_id(incoming_history_files)

    print(f'processing {len(grouped_incoming_history_files)} histories across {len(incoming_history_files)} incoming history files')

    # process each group, perform reward assignment, and upload rewarded decisions to s3
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=config.THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_incoming_history_file_group,
                          grouped_incoming_history_files))  # list() forces evaluation of generator

    print(f'uploaded {config.stats.rewarded_decision_count} rewarded decision records to s3://{config.TRAIN_BUCKET}')
    print(config.stats)
    print(f'finished reward assignment job node {config.NODE_ID}')


def process_incoming_history_file_group(file_group):
    if SIGTERM:
        print(f'Quitting due to SIGTERM signal (node {config.NODE_ID}).')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    History(file_group).process()
    

def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f"SIGTERM received (node {config.NODE_ID}).")
    return


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
