# Built-in imports
import gzip
import json
import os

# External imports
import pytest
import boto3
from moto import mock_s3

# Local imports
from utils import save_gzipped_jsonlines
from utils import upload_gzipped_jsonlines
from test_history_record import get_record

S3_BUCKET = "temp_bucket"
S3_KEY    = "temp_key"


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


def test_save_gzipped_jsonlines(tmp_path):
    """
    
    Parameters
    ----------
    tmp_path :  pathlib.Path
        A fixture automatically provided by Pytest
    """

    gzip_file = tmp_path / "temp_test.jsonl.gz"
    json_dicts = [
        get_record(),
        get_record(msg_val="B", type_val="event", model_val="m_name", count_val=2)
    ]
    
    save_gzipped_jsonlines(gzip_file, json_dicts)

    # Assert only number of files saved to temp dir
    assert len(list(tmp_path.iterdir())) == 1

    # Assert original contents are equal to saved contents
    with gzip.open(gzip_file, mode='r') as gzf:
        for i,line in enumerate(gzf.readlines()):
            loaded_dict = json.loads(line)
            original_dict = json_dicts[i]
            assert len(original_dict.keys()) == len(loaded_dict.keys())
            assert all([a == b for a, b in zip(original_dict.keys(), loaded_dict.keys())])



def test_test_upload_gzipped_jsonlines(s3, mocker):
    """

    Parameters
    ----------
    s3 : boto3.client
        Custom fixture which provides a mocked S3 client
    mocker : pytest_mock.plugin.MockerFixture
        Fixture provided by pytest-mocker
    """

    # Create a bucket in the Moto's virtual AWS account
    s3.create_bucket(Bucket=S3_BUCKET)

    # Replace the S3 client used in the app with a mock
    mocker.patch('config.s3client', new=s3)

    json_dicts = [
        get_record(),
        get_record(msg_val="B", type_val="event", model_val="m_name", count_val=2) ]

    upload_gzipped_jsonlines(S3_BUCKET, S3_KEY, json_dicts)

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY
    )

    with gzip.GzipFile(fileobj=response['Body']) as gzf:
        for i,line in enumerate(gzf.readlines()):
            loaded_dict = json.loads(line)
            original_dict = json_dicts[i]
            assert len(original_dict.keys()) == len(loaded_dict.keys())
            assert all([a == b for a, b in zip(original_dict.keys(), loaded_dict.keys())])


