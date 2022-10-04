import boto3
from io import BytesIO
from datetime import datetime
import os
import tarfile
import gzip
from uuid import uuid4

import src.train.constants as tc


def unpack(event, context):
    s3_client = boto3.client('s3')

    if not event['Records'][0]['s3']:
        raise TypeError('Invalid S3 event: {}'.format(event))

    s3_record = event['Records'][0]['s3']

    s3_bucket = s3_record['bucket']['name']
    s3_key = s3_record['object']['key']
    
    path_parts = s3_key.split('/')

    if path_parts[-1] != tc.EXPECTED_TRAINER_OUTPUT_FILENAME:
        raise ValueError(
            'Invalid S3 event - model filename `{}` differs from expected `{}`'
            .format(s3_key, tc.EXPECTED_TRAINER_OUTPUT_FILENAME))

    s3_model_name = path_parts[2]

    print(f'loading s3://{s3_bucket}/{s3_key}')
    models_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)

    if 'Body' not in models_object:
        raise ValueError('Missing `Body` in S3 `get_object()` response')

    # models_object['Body'] is a 'botocore.response.StreamingBody'
    # need to call read() on it to access the *.tar.gz file
    tar_gz_models_buffer = BytesIO(models_object['Body'].read())

    with tarfile.open(fileobj=tar_gz_models_buffer) as tar_gz_models_file:
        filenames = tar_gz_models_file.getnames()

        for filename in filenames:

            if filename == 'model.xgb':
                upload_extension = '.xgb.gz'
            elif filename == 'model.mlmodel':
                upload_extension = '.mlmodel.gz'
            else:
                print(f'skipping {filename}')
                continue

            key = get_timestamped_s3_key(model_name=s3_model_name, extension=upload_extension)
            latest_key = get_latest_s3_key(model_name=s3_model_name, extension=upload_extension)

            model_member = tar_gz_models_file.getmember(filename)
            model = tar_gz_models_file.extractfile(model_member).read()

            upload_model(
                key=key, latest_key=latest_key, s3_client=s3_client,
                model_fileobject=model)


def upload_model(key: str, latest_key: str, s3_client, model_fileobject: BytesIO):

    models_bucket = os.getenv(tc.MODELS_BUCKET_ENVVAR)
    
    # upload gzipped compressed model
    write_params = {
        'Fileobj': BytesIO(gzip.compress(model_fileobject)),
        'Bucket': models_bucket,
        'Key': key,
        'ExtraArgs': {
            'ContentType': 'application/gzip'
        }
    }

    copy_params = {
        'Bucket': models_bucket,
        'CopySource': models_bucket + '/' + key,
        'Key': latest_key,
    }

    print(f'writing s3://{models_bucket}/{key}')
    s3_client.upload_fileobj(**write_params)

    print(f'copying to s3://{models_bucket}/{latest_key}')
    s3_client.copy_object(**copy_params)

def get_latest_s3_key(model_name: str, extension: str) -> str:
    return f'models/latest/{model_name}{extension}'

def get_timestamped_s3_key(
        model_name: str, extension: str, model_uuid: str = None) -> str:
    date_str = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    return f'models/archive/{model_name}/{model_name}-{date_str}-{uuid4()}{extension}'
