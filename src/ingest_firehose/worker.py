import signal
import sys
import concurrent.futures
import pandas as pd
import itertools

from firehose_record import FirehoseRecordGroup
from rewarded_decisions import RewardedDecisionGroup, repair_overlapping_keys
from config import INCOMING_FIREHOSE_S3_KEY, TRAIN_BUCKET, THREAD_WORKER_COUNT, s3client, stats

SIGTERM = False


def worker():
    print(f'starting firehose ingest')
    
    # load the incoming firehose file and group records by model name
    firehose_record_groups = FirehoseRecordGroup.load_groups(INCOMING_FIREHOSE_S3_KEY)
    
    # create a flattened list of groups of incoming decisions to process
    decision_groups = list(itertools.chain.from_iterable(map(RewardedDecisionGroup.groups_from_firehose_record_group, firehose_record_groups)))
    
    print(f'processing {len(decision_groups)} groups of rewarded decisions...')
    
    # process each group. download the s3_key, consolidate records, upload rewarded decisions to s3, and delete old s3_key
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_decisions, decision_groups))  # list() forces evaluation of generator
    
    # if multiple ingests happen simultaneously it is possible for keys to overlap, which must be fixed
    repair_overlapping_keys(decision_groups)

    print(f'uploaded {stats.rewarded_decision_count} rewarded decision records to s3://{TRAIN_BUCKET}')
    print(stats)
    print(f'finished firehose ingest')


def process_decisions(decision_group: RewardedDecisionGroup):
    if SIGTERM:
        # this job is not automatically resumable, so hopefully the caller retries
        print(f'quitting due to SIGTERM signal')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    decision_group.process()
    

def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f'SIGTERM received')
    return

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
