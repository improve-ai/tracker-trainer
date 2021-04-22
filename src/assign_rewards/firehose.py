import boto3
import gzip
import json

import utils
import config

s3 = boto3.resource("s3")

# load an incoming firehose marker file, fetch the S3 target file, parse it and
# save the resulting incoming history files
def process_incoming_firehose_file(file):

    stats = utils.create_stats()

    records_by_history_id = {}
    error = None

    try:
        s3_bucket = None
        s3_key = None
        
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
                        stats[BAD_FIREHOSE_RECORD_HISTORY_ID_COUNT] += 1
                        
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
        # Unrecoverable parse error, copy file to /unrecoverable
        print(f'unrecoverable parse error "{error}", copying {file.absolute()} to {UNRECOVERABLE_PATH.absolute()}')
        stats[UNRECOVERABLE_PARSE_ERROR_COUNT] += 1
        copy_to_unrecoverable(file)
        
    # save each set of records to an incoming history file
    for history_id, records in records_by_history_id.items():
        hashed_history_id = utils.hash_history_id(history_id)
        output_file = utils.incoming_history_dir_for_hashed_history_id(hashed_history_id) / utils.unique_file_name_for_hashed_history_id(hashed_history_id)
        utils.save_gzipped_jsonlines(output_file.absolute(), records)
        stats[INCOMING_HISTORY_FILES_WRITTEN] += 1

    return stats


def select_incoming_firehose_files():
    # the file name begins with uuidv4, so the hash is the first 8 hex characters
    return utils.select_files_for_node(config.INCOMING_FIREHOSE_PATH, '*.json')
