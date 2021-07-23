import boto3
from io import BytesIO
from datetime import datetime
import os
import tarfile
from uuid import uuid4


AWS_S3_PATH_SEP = '/'
EXPECTED_TRAINER_OUTPUT_FILENAME = 'model.tar.gz'
EXPECTED_MODELS_COUNT = 2
MODELS_BUCKET_ENVVAR = 'MODELS_BUCKET'


def unpack(event, context):
    s3_client = boto3.client("s3")

    if not event["Records"][0]['s3']:
        raise TypeError('Invalid S3 event: {}'.format(event))

    s3_record = event['Records'][0]['s3']

    input_key = s3_record['object']['key']
    path_parts = input_key.split(AWS_S3_PATH_SEP)

    if path_parts[-1] != EXPECTED_TRAINER_OUTPUT_FILENAME:
        raise ValueError(
            'Invalid S3 event - model filename `{}` differs from expected `{}`'
            .format(input_key, EXPECTED_TRAINER_OUTPUT_FILENAME))

    # path_parts ==
    # ['models', 'dummy-model-1',
    # 'improve-train-job-v6-dummy-model-1-20210722122235', 'output',
    # 'model.tar.gz']
    _, s3_model_name, train_job_name, _, model_filename = path_parts

    params = {
        'Bucket': s3_record['bucket']['name'],
        'Key': input_key}

    models_object = s3_client.get_object(**params)

    if "Body" not in models_object:
        raise ValueError('Missing `Body` in S3 `get_object()` response')

    # models_object["Body"] is a 'botocore.response.StreamingBody'
    # need to call read() on it to access the *.tar.gz file
    tar_gz_models_buffer = BytesIO(models_object["Body"].read())

    with tarfile.open(fileobj=tar_gz_models_buffer) as tar_gz_models_file:
        model_filenames = tar_gz_models_file.getnames()
        if len(model_filenames) != EXPECTED_MODELS_COUNT:
            raise ValueError(
                'Expected: {} models, got: {}'
                .format(EXPECTED_MODELS_COUNT, len(model_filenames)))

        trained_models_uuid = str(uuid4())

        for model_filename in model_filenames:
            extension = get_extension(model_filename)

            if not extension:
                continue

            key = \
                get_timestamped_s3_key(
                    model_name=s3_model_name, extension=extension,
                    model_uuid=trained_models_uuid)
            latest_key = \
                get_latest_s3_key(
                    model_name=s3_model_name, extension=extension)

            model_member = tar_gz_models_file.getmember(model_filename)
            model = tar_gz_models_file.extractfile(model_member)

            upload_model(
                key=key, latest_key=latest_key, s3_client=s3_client,
                uploaded_model_fileobject=model)


def upload_model(
        key: str, latest_key: str, s3_client, uploaded_model_fileobject: BytesIO):

    write_params = {
        'Fileobj': uploaded_model_fileobject,
        'Bucket': os.getenv(MODELS_BUCKET_ENVVAR),
        'Key': key,
        'ExtraArgs': {
            'ContentType':
                'application/protobuf' if key.split('.')[-1] == 'mlmodel'
                else 'application/gzip'}}

    copy_params = {
        'Bucket': os.getenv(MODELS_BUCKET_ENVVAR),
        'CopySource': os.getenv(MODELS_BUCKET_ENVVAR) + AWS_S3_PATH_SEP + key,
        'Key': latest_key,
    }

    s3_client.upload_fileobj(**write_params)
    s3_client.copy_object(**copy_params)


def get_extension(filename: str) -> str or None:
    split_filename = filename.split('.')

    extracted_extension = '.'.join(split_filename[1:])

    if extracted_extension == 'xgb':
        return '.xgb'
    elif extracted_extension == 'xgb.gz':
        return '.xgb.gz'
    elif extracted_extension == 'mlmodel':
        return '.mlmodel'
    elif extracted_extension == 'mlmodel.gz':
        return '.mlmodel.gz'
    else:
        return


# TODO ask if removal of project_name is ok
def get_latest_s3_key(model_name: str, extension: str) -> str:
    return f'models/latest/improve-{model_name}{extension}'


# TODO ask if removal of project_name is ok
def get_timestamped_s3_key(
        model_name: str, extension: str, model_uuid: str = None) -> str:
    date_str = datetime.now().strftime('%Y-%m-%d-%M-%H-%S')

    if model_uuid is None:
        model_uuid = str(uuid4())

    return f'models/archive/{model_name}/improve-{model_name}-{date_str}-' \
           f'{model_uuid}{extension}'
