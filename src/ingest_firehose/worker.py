import os
import signal
import sys

from firehose import load_firehose_file
from config import FIREHOSE_BUCKET, INCOMING_FIREHOSE_S3_KEY

SIGTERM = False


def worker():
    """
    Download a gzipped JSONL file from S3, where each line is a record. 
    Generate a new gzipped JSONL file for each different HISTORY_ID_KEY
    where each line is a record.
    """
    
    print(f'starting firehose ingest for s3://{FIREHOSE_BUCKET}/{INCOMING_FIREHOSE_S3_KEY}')
    
    load_firehose_file(FIREHOSE_BUCKET, INCOMING_FIREHOSE_S3_KEY)


def process_history(history: History):
    if SIGTERM:
        # this process is not automatically resumable, so hopefully the caller decides to retry
        print(f'quitting due to SIGTERM signal')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    history.process()
    

def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f'SIGTERM received')
    return

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
