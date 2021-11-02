import signal
import sys
import concurrent.futures
import pandas as pd 

from firehose import load_firehose_records
from config import FIREHOSE_BUCKET, INCOMING_FIREHOSE_S3_KEY, TRAIN_BUCKET, THREAD_WORKER_COUNT, s3client

SIGTERM = False


def worker():
    """
    Download a gzipped JSONL file from S3, where each line is a record. 
    """
    
    print(f'starting firehose ingest for s3://{FIREHOSE_BUCKET}/{INCOMING_FIREHOSE_S3_KEY}')
    
    # load the incoming tracked records from firehose
    tracked_records = load_tracked_records(FIREHOSE_BUCKET, INCOMING_FIREHOSE_S3_KEY)
    
    # convert the tracked records to rewarded decision record dicts
    records = map(lambda x: x.to_rewarded_decision_dict(), tracked_records)
    
    # group the records with existing rewarded decision s3 keys
    grouped_records, s3_keys = group_records_with_s3_keys(decision_records)
    
    print(f'processing {len(grouped_records)} groups of records...')
    
    # process each group. download the s3_key, consolidate records, upload rewarded decisions to s3, and delete old s3_key
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKER_COUNT) as executor:
        list(executor.map(process_group, grouped_records, s3_keys))  # list() forces evaluation of generator
        
    # TODO cleanup_inconsistent_keys(grouped_records)

    print(f'uploaded {config.stats.rewarded_decision_count} rewarded decision records to s3://{TRAIN_BUCKET}')
    print(config.stats)
    print(f'finished firehose ingest')



def process_group(records: list, s3_key: str):
    if SIGTERM:
        # this job is not automatically resumable, so hopefully the caller retries
        print(f'quitting due to SIGTERM signal')
        sys.exit()  # raises SystemExit, so worker threads should have a chance to finish up

    # convert records to a dataframe
    df = pd.DataFrame(records)
    
    if s3_key is not None:
        # load the existing parquet file
        s3_df = pd.read_parquet(f's3://{TRAIN_BUCKET}/{s3_key}')
        df = consolidate_dataframes(records_df, s3_df)

    # write the conslidated parquet file to a unique key
    df.write_parquet(f's3://{TRAIN_BUCKET}/{s3_key_for_dataframe(df)}')

    if s3_key is not None:
        # clean up the old s3_key
        s3client.delete_object(Bucket=TRAIN_BUCKET, Key=s3_key)

    

def signal_handler(signalNumber, frame):
    global SIGTERM
    SIGTERM = True
    print(f'SIGTERM received')
    return

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    worker()
