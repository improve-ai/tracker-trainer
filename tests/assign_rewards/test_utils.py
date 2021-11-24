# # Built-in imports
# import gzip
# import json
# import os
#
# # Local imports
# from utils import save_gzipped_jsonlines
# from utils import upload_gzipped_jsonlines
# from utils import copy_file, delete_all, ensure_parent_dir
#
#
# S3_BUCKET = "temp_bucket"
# S3_KEY    = "temp_key"
#
#
# def test_save_gzipped_jsonlines(get_record, tmp_path):
#     """
#
#     Parameters
#     ----------
#     get_record : function
#         Custom fixture which returns a record
#     tmp_path :  pathlib.Path
#         A fixture automatically provided by Pytest
#     """
#
#     gzip_file = tmp_path / "temp_test.jsonl.gz"
#     json_dicts = [
#         get_record(),
#         get_record(msg_val="B", type_val="event", model_val="m_name", count_val=2)
#     ]
#
#     save_gzipped_jsonlines(gzip_file, json_dicts)
#
#     # Assert only number of files saved to temp dir
#     assert len(list(tmp_path.iterdir())) == 1
#
#     # Assert original contents are equal to saved contents
#     with gzip.open(gzip_file, mode='r') as gzf:
#         for i,line in enumerate(gzf.readlines()):
#             loaded_dict = json.loads(line)
#             original_dict = json_dicts[i]
#             assert len(original_dict.keys()) == len(loaded_dict.keys())
#             assert all([a == b for a, b in zip(original_dict.keys(), loaded_dict.keys())])
#
#
# def test_test_upload_gzipped_jsonlines(get_record, s3, mocker):
#     """
#
#     Parameters
#     ----------
#     get_record : function
#         Custom fixture which returns a record
#     s3 : boto3.client
#         Custom fixture which provides a mocked S3 client
#     mocker : pytest_mock.MockerFixture
#         Fixture provided by pytest-mocker
#     """
#
#     # Create a bucket in the Moto's virtual AWS account
#     s3.create_bucket(Bucket=S3_BUCKET)
#
#     # Replace the S3 client used in the app with a mock
#     mocker.patch('config.s3client', new=s3)
#
#     json_dicts = [
#         get_record(),
#         get_record(msg_val="B", type_val="event", model_val="m_name", count_val=2) ]
#
#     upload_gzipped_jsonlines(S3_BUCKET, S3_KEY, json_dicts)
#
#     response = s3.get_object(
#         Bucket=S3_BUCKET,
#         Key=S3_KEY
#     )
#
#     with gzip.GzipFile(fileobj=response['Body']) as gzf:
#         for i,line in enumerate(gzf.readlines()):
#             loaded_dict = json.loads(line)
#             original_dict = json_dicts[i]
#             assert len(original_dict.keys()) == len(loaded_dict.keys())
#             assert all([a == b for a, b in zip(original_dict.keys(), loaded_dict.keys())])
#
#
# def test_copy_file(tmp_path):
#     """
#
#     Parameters
#     ----------
#     tmp_path :  pathlib.Path
#         A fixture automatically provided by Pytest
#     """
#
#     input_path = (tmp_path / "input")
#     input_path.mkdir()
#     input_f = input_path / "tmp_input.txt"
#     input_f.write_text("CONTENT")
#
#     output_path = (tmp_path / "output")
#     output_path.mkdir()
#     output_f = output_path / "tmp_output.txt"
#
#     copy_file(input_f, output_f)
#
#     assert output_f.read_text() == "CONTENT"
#     assert len(list(output_path.iterdir())) == 1
#
#
# def test_delete_all(tmp_path):
#     """
#
#     Parameters
#     ----------
#     tmp_path :  pathlib.Path
#         A fixture automatically provided by Pytest
#     """
#
#     files = []
#     for i in range(3):
#         f = tmp_path / f"tmp_file_{i}.txt"
#         f.write_text(f"CONTENT {i}")
#         files.append(f)
#
#     delete_all(tmp_path.iterdir())
#
#     for f in files: assert not f.exists()
#
#
# def test_ensure_parent_dir(tmp_path):
#     """
#
#     Parameters
#     ----------
#     tmp_path :  pathlib.Path
#         A fixture automatically provided by Pytest
#     """
#
#     f = (tmp_path / "not_existent" / "tmp.txt")
#     ensure_parent_dir(f)
#     assert (tmp_path / "not_existent").exists()