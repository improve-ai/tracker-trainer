import sys
import signal
import time
import concurrent.futures
import boto3
import botocore

import histories
import config
import utils
import stats

SIGTERM = False

stats = stats.Stats()

# boto3 client must be pre-initialized for multi-threaded (https://github.com/boto/botocore/issues/1246)
s3client = boto3.client("s3", config=botocore.config.Config(max_pool_connections=config.THREAD_WORKER_COUNT))

# execute the core worker loop of processing incoming firehose files then 
# processing incoming history files
def worker():
    print(f"starting AWS Batch array job on node {config.NODE_ID} ({config.NODE_COUNT} nodes total)")


    # identify the portion of incoming history files to process in this node
    incoming_history_files = histories.select_incoming_history_files()

    print(f'processing {len(incoming_history_files)} incoming history files')
            
    # group the incoming files by their hashed history ids
    grouped_incoming_history_files = histories.group_files_by_hashed_history_id(incoming_history_files)

    # process each group, perform reward assignment, and upload rewarded decisions to s3
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_incoming_history_file_group, grouped_incoming_history_files)) # list() forces evaluation of generator

    print(stats)
    print(f"batch array node {config.NODE_ID} finished.")


def process_incoming_history_file_group(file_group):
    handle_signals()
    histories.process_incoming_history_file_group(file_group)

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
