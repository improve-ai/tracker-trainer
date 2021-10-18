# Built-in imports
import io
import json
import gzip
import shutil
import re

# Local imports
import config


def ensure_parent_dir(file):
    parent_dir = file.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)


def copy_file(src, dest):
    ensure_parent_dir(dest)
    shutil.copy(src.absolute(), dest.absolute())


def save_gzipped_jsonlines(file, json_dicts):
    with gzip.open(file, mode='w') as gzf:
        for json_dict in json_dicts:
            gzf.write((json.dumps(json_dict) + '\n').encode())


def upload_gzipped_jsonlines(s3_bucket, s3_key, json_dicts):
    gzipped = io.BytesIO()

    save_gzipped_jsonlines(gzipped, json_dicts)

    gzipped.seek(0)

    config.s3client.put_object(Bucket=s3_bucket, Body=gzipped, Key=s3_key)


def delete_all(paths):
    for path in paths:
        path.unlink(missing_ok=True)

