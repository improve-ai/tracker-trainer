import sys
import signal
import concurrent.futures

import config
import utils
import history
from history import History

SIGTERM = False


def worker():
    print(f'starting reward assignment job on node {config.NODE_ID} ({config.NODE_COUNT} nodes total)')

    histories = history.histories_to_process()
    
    print(f'processing {len(histories)} histories')
    
    # process each history, perform reward assignment, and upload rewarded decisions to s3
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_history, histories))  # list() forces evaluation of generator

    print(f'uploaded {config.stats.rewarded_decision_count} rewarded decision records to s3://{config.TRAIN_BUCKET}')
    print(config.stats)
    print(f'finished reward assignment job node {config.NODE_ID}')


def process_history(history: History):
    if SIGTERM:
        print(f'quitting due to SIGTERM signal (node {config.NODE_ID}).')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    history.process()
    

def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f"SIGTERM received (node {config.NODE_ID}).")
    return


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
