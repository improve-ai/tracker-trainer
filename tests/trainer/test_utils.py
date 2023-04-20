from dateutil import parser
import orjson
import os

import src.trainer.code.constants as tc


def get_valid_timestamp(timestamp):
    """ Return a parsed and validated timestamp"""

    # if missing / None raise
    assert timestamp is not None

    # if not string raise
    assert isinstance(timestamp, str)

    # check string format
    try:
        parsed_timestamp = parser.isoparse(timestamp)
    except ValueError:
        parsed_timestamp = None

    assert parsed_timestamp is not None

    return parsed_timestamp


def load_test_case_json(test_case_file: str, test_subdir: str = None):
    # get master test data dir
    test_data_dir = os.getenv('TEST_DATA_DIR', None)
    assert test_data_dir is not None
    assert test_case_file is not None

    # join all into path
    test_case_path = os.sep.join(([test_data_dir, test_subdir] if test_subdir is not None else []) + [test_case_file])
    with open(test_case_path, 'r') as tcpf:
        test_case = orjson.loads(tcpf.read())

    return test_case


def decode_json_columns_in_test_df(df, columns):

    for key in [tc.ITEM_KEY, tc.CONTEXT_KEY, tc.SAMPLE_KEY]:
        if key in columns:
            df[key] = df[key].apply(lambda x: orjson.loads(x) if x is not None else None)

    return df
