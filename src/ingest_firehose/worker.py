import concurrent.futures
import itertools
import random
import signal
import sys
import time
import traceback
import threading

from config import INCOMING_FIREHOSE_S3_KEY, TRAIN_BUCKET, THREAD_WORKER_COUNT, stats, DEBUG
from firehose_record import FirehoseRecordGroup
from rewarded_decisions import RewardedDecisionPartition, repair_overlapping_keys

SIGTERM = False


def worker():
    if DEBUG:
        print(f'Starting firehose ingest')
    
    # load the incoming firehose file and group records by model name
    firehose_record_groups = FirehoseRecordGroup.load_groups(INCOMING_FIREHOSE_S3_KEY)
    
    if DEBUG:
        print("Loading RewardedDecisionPartition(s) from FirehoseRecordGroup(s)")
    # create a flattened list of groups of incoming decisions to process
    decision_partitions = list(itertools.chain.from_iterable(map(RewardedDecisionPartition.partitions_from_firehose_record_group, firehose_record_groups)))
    
    # process each group. download the s3_key, consolidate records, upload rewarded decisions to s3, and delete old s3_key
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_decisions, decision_partitions))  # list() forces evaluation of generator
    
    total_from_partitions, total_from_s3 = stats.store.summarize_all()
    if DEBUG:
        print("Total (P)RDRs from JSONL: {}; Total (P)RDRs from Parquet files in S3: {}".format(
            total_from_partitions, total_from_s3))
        print(f"Total (P)RDRs after merge: {stats.records_after_merge_count}")
    
    # stats.timer.start('repair overlapping keys')
    # if multiple ingests happen simultaneously it is possible for keys to overlap, which must be fixed
    sort_key = lambda x: x.model_name
    for model_name, model_decision_partitions in itertools.groupby(sorted(decision_partitions, key=sort_key), sort_key):
        # execute each serially in one thread to have maximum memory available for loading overlapping partitions
        repair_overlapping_keys(model_name, model_decision_partitions)
    # stats.timer.stop('repair overlapping keys')

    if sum(stats.counts_of_set_of_overlapping_s3_keys) > 0:
        if DEBUG:
            print("{} overlapping keys turned into {} keys".format(
                sum(stats.counts_of_set_of_overlapping_s3_keys),
                len(stats.counts_of_set_of_overlapping_s3_keys)
            ))

    if DEBUG:
        print(f'Finished firehose ingest')
    
    print(stats)


def process_decisions(decision_partition: RewardedDecisionPartition):
    if SIGTERM:
        print(f"Due to a received SIGTERM, this thread won't start its work")
        return

    decision_partition.process()


def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f'SIGTERM received')

    # From https://stackoverflow.com/a/24334576/1253729
    for th in threading.enumerate():
        print(th)
        traceback.print_stack(sys._current_frames()[th.ident])
        print()

    # What sys.exit() does with multiple threads:
    # https://stackoverflow.com/a/38805873/1253729
    sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
