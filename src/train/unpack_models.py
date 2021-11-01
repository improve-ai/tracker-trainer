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

    input_key = s3_record['object']['key']
    path_parts = input_key.split('/')

    if path_parts[-1] != tc.EXPECTED_TRAINER_OUTPUT_FILENAME:
        raise ValueError(
            'Invalid S3 event - model filename `{}` differs from expected `{}`'
            .format(input_key, tc.EXPECTED_TRAINER_OUTPUT_FILENAME))

    s3_model_name = path_parts[1]

    params = {
        'Bucket': s3_record['bucket']['name'],
        'Key': input_key}

    models_object = s3_client.get_object(**params)

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


def upload_model(
        key: str, latest_key: str, s3_client, model_fileobject: BytesIO):

    # upload gzipped compressed model
    write_params = {
        'Fileobj': BytesIO(gzip.compress(model_fileobject)),
        'Bucket': os.getenv(tc.MODELS_BUCKET_ENVVAR),
        'Key': key,
        'ExtraArgs': {
            'ContentType': 'application/gzip'
        }
    }

    copy_params = {
        'Bucket': os.getenv(tc.MODELS_BUCKET_ENVVAR),
        'CopySource': os.getenv(tc.MODELS_BUCKET_ENVVAR) + '/' + key,
        'Key': latest_key,
    }

    s3_client.upload_fileobj(**write_params)
    s3_client.copy_object(**copy_params)

def get_latest_s3_key(model_name: str, extension: str) -> str:
    return f'models/latest/{model_name}{extension}'

def get_timestamped_s3_key(
        model_name: str, extension: str, model_uuid: str = None) -> str:
    date_str = datetime.now().strftime('%Y-%m-%d-%M-%H-%S')

    return f'models/archive/{model_name}/{model_name}-{date_str}-{uuid4()}{extension}'
