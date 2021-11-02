import signal
import sys
import concurrent.futures
import pandas as pd 

from firehose_records import FirehoseRecords
from rewarded_decisions import RewardedDecisions
from config import INCOMING_FIREHOSE_S3_KEY, TRAIN_BUCKET, THREAD_WORKER_COUNT, s3client, stats

SIGTERM = False


def worker():
    print(f'starting firehose ingest')
    
    # load the incoming firehose file
    firehose_records = FirehoseRecords(INCOMING_FIREHOSE_S3_KEY)
    firehose_records.load()
    
    # create groups of incoming decisions to process
    decisions_groups = firehose_records.to_rewarded_decisions_groups()
    
    print(f'processing {len(decisions_groups)} groups of rewarded decisions...')
    
    # process each group. download the s3_key, consolidate records, upload rewarded decisions to s3, and delete old s3_key
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_decisions, decisions_groups))  # list() forces evaluation of generator
    
    # if multiple ingests happen simultaneously it is possible for keys to overlap, which must be fixed
    rewarded_decisions.repair_overlapping_keys()

    print(f'uploaded {stats.rewarded_decision_count} rewarded decision records to s3://{TRAIN_BUCKET}')
    print(stats)
    print(f'finished firehose ingest')


def process_decisions(decisions: RewardedDecisions):
    if SIGTERM:
        # this job is not automatically resumable, so hopefully the caller retries
        print(f'quitting due to SIGTERM signal')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    decisions.process()
    

def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f'SIGTERM received')
    return

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
