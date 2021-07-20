# Built-in imports
import io
import json
import re
import sys
import hashlib
import gzip
import zlib
import shutil
import uuid
import dateutil
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from itertools import groupby

import config
import constants


def load_history(file_group):
    records = []
    message_ids = set()

    for file in file_group:
        records.extend(load_records(file, message_ids))

    return records


def load_records(file, message_ids):
    """
    Load a gzipped jsonlines file

    Args:
        file: Path of the input gzipped jsonlines file to load

    Returns:
        A list of records
    """

    line_count = 0

    records = []
    error = None

    try:
        with gzip.open(file.absolute(), mode="rt", encoding="utf8") as gzf:
            for line in gzf.readlines():
                line_count += 1  # count every line as a record whether it's parseable or not
                # Do a inner try/except to try to recover as many records as possible
                try:
                    record = json.loads(line)
                    # parse the timestamp into a datetime since it will be used often
                    record[constants.TIMESTAMP_KEY] = \
                        dateutil.parser.parse(record[constants.TIMESTAMP_KEY])

                    message_id = record[constants.MESSAGE_ID_KEY]
                    if message_id not in message_ids:
                        message_ids.add(message_id)
                        records.append(record)
                    else:
                        config.stats.incrementDuplicateMessageIdCount()
                except (json.decoder.JSONDecodeError, ValueError) as e:
                    error = e
    except (zlib.error, EOFError, gzip.BadGzipFile) as e:
        # gzip can throw zlib.error, EOFError, or gzip.BadGZipFile on corrupt file
        error = e

    if error:
        # Unrecoverable parse error, copy the file to /unrecoverable
        dest = config.UNRECOVERABLE_PATH / file.name
        print(
            f'unrecoverable parse error "{error}", copying {file.absolute()} to {dest.absolute()}')
        copy_file(file, dest)
        config.stats.incrementUnrecoverableFileCount()

    config.stats.incrementHistoryRecordCount(line_count)

    return records


def select_incoming_history_files():
    # hash based on the first 8 characters of the hashed history id
    # return select_files_for_node(config.INCOMING_PATH, '*.jsonl.gz')
    # TODO check valid file name & hashed history id chars
    files_for_node = select_files_for_node(config.INCOMING_PATH, '*.jsonl.gz')
    return files_for_node


def save_history(hashed_history_id, history_records):
    output_file = \
        history_dir_for_hashed_history_id(
            hashed_history_id) / f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'

    ensure_parent_dir(output_file)

    save_gzipped_jsonlines(output_file.absolute(), history_records)


def unique_hashed_history_file_name(hashed_history_id):
    return f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'


def hashed_history_id_from_file(file):
    return file.name.split('-')[0]


def history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return config.HISTORIES_PATH / hashed_history_id[0:2] / hashed_history_id[
                                                            2:4]


def history_files_for_hashed_history_id(hashed_history_id):
    results = \
        list(
            history_dir_for_hashed_history_id(hashed_history_id)
                .glob(f'{hashed_history_id}-*.jsonl.gz'))
    return results


def group_files_by_hashed_history_id(files):
    sorted_files = sorted(files, key=hashed_history_id_from_file)
    return [list(it) for k, it in groupby(
        sorted_files, hashed_history_id_from_file)]


def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)


def copy_file(src, dest):
    ensure_parent_dir(dest)
    shutil.copy(src.absolute(), dest.absolute())


def _is_history_filename_valid(checked_filename: str):
    """
    Checks:
     - if extension jsonl.gz is present
     - length of first 'chunk' of name split by '-' (sha256 check)
     - length of all remaining chunks (uuid4) check
     length and allows only for alnum characters in provided filename
    Desired name pattern looks as follows:
    00eda666cee662eef503f3ab4b7d6375ceda6549821265fa4a5081ce46d4cbb1-202342f0-ee10-40d5-9207-7ab785187f0a.jsonl.gz
    """

    # bad extension
    if re.match(
            constants.JSONLINES_FILENAME_EXTENSION_REGEXP, checked_filename) is None:
        # Filename has no jsonl.gz suffix
        return False

    sha_filename_chunk = checked_filename.split('-')[0]

    # illegal chars or bad length of sha256 chunk
    if len(sha_filename_chunk) != constants.SHA_256_LEN \
            or re.match(
            constants.HISTORY_FILE_NAME_REGEXP, sha_filename_chunk) is None:
        # Filename has illegal chars in hashed history id
        return False

    uuid4_filename_chunk = \
        '-'.join(checked_filename.split('-')[1:]).split('.')[0]
    # illegal chars or bad length of uuid4 chunk
    if len(uuid4_filename_chunk) != constants.UUID4_LENGTH \
            or re.match(
                constants.UUID4_FILE_NAME_REGEXP, uuid4_filename_chunk) is None:
        # Filename has illegal chars in uuid4
        return False

    return True


def select_files_for_node(input_dir, glob):
    files_to_process = []
    file_count = 0
    for f in input_dir.glob(glob):
        # TODO bad file names stats
        if not _is_history_filename_valid(checked_filename=f.name):
            # increment  bad file names stats
            config.stats.incrementUnrecoverableFileCount()
            continue

        file_count += 1
        # convert first 8 hex chars (32 bit) to an int
        # the file name starts with a uuidv4
        # check if int mod node_count matches our node
        if (int(f.name[:8], 16) % config.NODE_COUNT) == config.NODE_ID:
            files_to_process.append(f)

    print(
        f'selected {len(files_to_process)} of {file_count} files from {input_dir}/{glob} to process')

    return files_to_process


def save_gzipped_jsonlines(file, records):
    with gzip.open(file, mode='w') as gzf:
        for record in records:
            gzf.write((json.dumps(record, default=serialize_datetime) + "\n")
                      .encode())


def upload_gzipped_jsonlines(s3_bucket, s3_key, records):
    gzipped = io.BytesIO()

    save_gzipped_jsonlines(gzipped, records)

    gzipped.seek(0)

    config.s3client.put_object(Bucket=s3_bucket, Body=gzipped, Key=s3_key)


def drop_needless_keys_from_records(rewarded_decisions):
    rewarded_decisions_with_desired_keys = \
        [{k: rd[k] for k in rd if k in constants.REWARDED_DECISIONS_S3_KEYS}
         for rd in rewarded_decisions]

    return rewarded_decisions_with_desired_keys


def upload_rewarded_decisions(model, hashed_history_id, rewarded_decisions):
    # TODO double check model name and hashed_history_id to ensure valid characters

    # check hashed_historyid for sha256
    if re.match(constants.HISTORY_FILE_NAME_REGEXP, hashed_history_id) is None:
        # Malformed `hashed_history_id`
        return

    # check model name
    if re.match(constants.MODEL_NAME_REGEXP, model) is None:
        # 'Malformed `model` name
        return

    # TODO does this return statement have eny effect ?
    return upload_gzipped_jsonlines(
        config.TRAIN_BUCKET,
        rewarded_decisions_s3_key(
            model, hashed_history_id), rewarded_decisions)


def delete_all(paths):
    for path in paths:
        path.unlink(missing_ok=True)


def rewarded_decisions_s3_key(model, hashed_history_id):
    return f'rewarded_decisions/{model}/{hashed_history_id[0:2]}/{hashed_history_id[2:4]}/{hashed_history_id}.jsonl.gz'


def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f'{type(obj)} not serializable')


def hash_history_id(history_id):
    return hashlib.sha256(history_id.encode()).hexdigest()


def sort_records_by_timestamp(records):
    def sort_key(x):
        timestamp = datetime.strptime(x['timestamp'], config.DATETIME_FORMAT)
        if x['type'] in ('rewards', 'event'):
            timestamp += timedelta(seconds=1)
        return timestamp

    records.sort(key=sort_key)

    return records


def deepcopy(o):
    return json.loads(json.dumps(o))

