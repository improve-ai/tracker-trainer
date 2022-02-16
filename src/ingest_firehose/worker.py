import concurrent.futures
import itertools
import random
import signal
import sys
import time

from config import BATCH_JOB_ATTEMPT, INCOMING_FIREHOSE_S3_KEY, TRAIN_BUCKET, THREAD_WORKER_COUNT, stats
from firehose_record import FirehoseRecordGroup
from rewarded_decisions import RewardedDecisionPartition, repair_overlapping_keys


SIGTERM = False


def worker():
    if BATCH_JOB_ATTEMPT > 1:
        # the previous batch job failed. perform randomized exponential back off before retrying
        backoff()
        
    print(f'starting firehose ingest')
    
    # load the incoming firehose file and group records by model name
    firehose_record_groups = FirehoseRecordGroup.load_groups(INCOMING_FIREHOSE_S3_KEY)
    
    # create a flattened list of groups of incoming decisions to process
    decision_partitions = list(itertools.chain.from_iterable(map(RewardedDecisionPartition.partitions_from_firehose_record_group, firehose_record_groups)))
    
    print(f'processing {len(decision_partitions)} groups of rewarded decisions...')
    
    # process each group. download the s3_key, consolidate records, upload rewarded decisions to s3, and delete old s3_key
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_decisions, decision_partitions))  # list() forces evaluation of generator

    print(f'uploaded {stats.rewarded_decision_count} rewarded decision records to s3://{TRAIN_BUCKET}')

    # if multiple ingests happen simultaneously it is possible for keys to overlap, which must be fixed
    sort_key = lambda x: x.model_name
    for model_name, model_decision_partitions in itertools.groupby(sorted(decision_partitions, key=sort_key), sort_key):
        # execute each serially in one thread to have maximum memory available for loading overlapping partitions
        repair_overlapping_keys(model_name, model_decision_partitions)

    print(stats)
    print(f'finished firehose ingest')


def process_decisions(decision_partition: RewardedDecisionPartition):
    if SIGTERM:
        # this job is not automatically resumable, so hopefully the caller retries
        print(f'quitting due to SIGTERM signal')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    decision_partition.process()
    

def backoff():
    # the base backoff is beween 0 and 60 seconds, with the window doubling with each attempt
    backoff_seconds = 60 * (2 ** (BATCH_JOB_ATTEMPT - 2)) * random.random()
    print(f'job attempt {BATCH_JOB_ATTEMPT}, waiting {backoff_seconds} seconds before retrying')
    time.sleep(backoff_seconds)


def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f'SIGTERM received')
    return


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
