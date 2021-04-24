import boto3
import gzip
import json
import shutil

import utils
import config
import worker

s3 = boto3.resource("s3") # TODO replace with worker.s3client

# load an incoming firehose marker file, fetch the S3 target file, parse it and
# save the resulting incoming history files
def process_incoming_firehose_file(file):

    records_by_history_id = {}
    error = None

    s3_bucket = None
    s3_key = None

    try:
        # parse the marker file and extract the s3 bucket and key
        with open(file) as f:
            data = json.load(f)
            s3_bucket = data['s3_bucket']
            s3_key = data['s3_key']

        # download and parse the firehose file
        obj = s3.Object(s3_bucket, s3_key)
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzf:
            for line in gzf.readlines():
                # Do a inner try/except to try to recover as many records as possible
                try: 
                    record = json.loads(line)
                    
                    history_id = record.get('history_id', None)
                    if not history_id or isinstance(history_id, str) or not len(history_id):
                        worker.stats.incrementBadFirehoseRecordCount()
                        
                    if not history_id in records_by_history_id:
                        records_by_history_id[history_id] = []
                        
                    records_by_history_id[history_id].append(record)
                    
                except (json.decoder.JSONDecodeError, ValueError) as e:
                    error = e
    except (zlib.error, EOFError, gzip.BadGzipFile, json.decoder.JSONDecodeError, ValueError) as e:
        # gzip can throw zlib.error, EOFError, or gzip.BadGZipFile on corrupt file
        # marker file parsing can throw JSONDecodeError or ValueError
        error = e
        
    if error:
        # Unrecoverable parse error, copy the marker file to /unrecoverable
        dest = config.UNRECOVERABLE_INCOMING_FIREHOSE_PATH / file.name
        print(f'unrecoverable parse error "{error}" for s3 bucket {s3_bucket} s3 key {s3_key}, copying {file.absolute()} to {dest.absolute()}')
        utils.ensure_parent_dir(dest)
        shutil.copy(file.absolute(), dest.absolute())
        worker.stats.incrementUnrecoverableFileCount()
        
    # save each set of records to an incoming history file
    for history_id, records in records_by_history_id.items():
        hashed_history_id = utils.hash_history_id(history_id)
        output_file = utils.incoming_history_dir_for_hashed_history_id(hashed_history_id) / utils.unique_file_name_for_hashed_history_id(hashed_history_id)
        utils.save_gzipped_jsonlines(output_file.absolute(), records)
        worker.stats.incrementIncomingHistoryFilesWrittenCount()


def select_incoming_firehose_files():
    # the file name begins with uuidv4, so the hash is the first 8 hex characters
    return utils.select_files_for_node(config.INCOMING_FIREHOSE_PATH, '*.json')
