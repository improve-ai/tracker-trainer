# Built-in imports
import io
import json
import re
import sys
import hashlib
import gzip
import zlib
import shutil
from uuid import uuid4
import dateutil
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import config
import constants
import utils



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


def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)


def copy_file(src, dest):
    ensure_parent_dir(dest)
    shutil.copy(src.absolute(), dest.absolute())


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


def delete_all(paths):
    for path in paths:
        path.unlink(missing_ok=True)

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f'{type(obj)} not serializable')

def deepcopy(o):
    return json.loads(json.dumps(o))
