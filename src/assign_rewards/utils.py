# Built-in imports
import io
import json
import gzip
import shutil
from datetime import datetime

import config
import constants

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
