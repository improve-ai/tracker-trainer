import signal
import concurrent.futures

import firehose
import config
import utils

NODE_ID = config.AWS_BATCH_JOB_ARRAY_INDEX
NODE_COUNT = config.REWARD_ASSIGNMENT_WORKER_COUNT

def worker():
    print(f"starting AWS Batch array job on node {NODE_ID} ({NODE_COUNT} nodes total)")

    job_stats = utils.create_stats()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.THREAD_WORKER_COUNT) as executor:
        while True:
            # identify the portion of incoming firehose files to process in this node
            incoming_firehose_files = firehose.select_incoming_firehose_files(job_stats)

            # process each incoming firehose marker file
            for group_stats in executor.map(firehose.process_incoming_firehose_file, incoming_firehose_files):
                # accumulate the stats
                for key, value in group_stats.items():
                    job_stats[key] += value
    
            # identify the portion of incoming history files to process in this node
            incoming_history_files = histories.select_incoming_history_files(job_stats)
            
            if not len(incoming_history_files):
                # nothing more to process, break
                break
            
            # group the incoming files by their hashed history ids
            grouped_incoming_history_files = histories.group_files_by_hashed_history_id(incoming_history_files)
        
            # process each group
            for group_stats in executor.map(process_incoming_history_file_group, grouped_incoming_history_files):
                # accumulate the stats
                for key, value in group_stats.items():
                    job_stats[key] += value

    print(job_stats)
    print(f"batch array node {NODE_ID} finished.")


def handle_signals():
    if SIGTERM:
        # TODO throw exception instead of sys.exit() so that already active workers can finish
        print(f'Quitting due to SIGTERM signal (node {NODE_ID}).')
        sys.exit()


def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f"SIGTERM received (node {NODE_ID}).")
    return


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
