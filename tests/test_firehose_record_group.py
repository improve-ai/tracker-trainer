from copy import deepcopy
import dateutil
import orjson
import pandas as pd
from ksuid import Ksuid
import numpy as np

import firehose_record as fr


TEST_MODEL_NAME = 'test-model'
RECORD_PATTERN = {
    fr.MESSAGE_ID_KEY: None,
    fr.TIMESTAMP_KEY: None,
    fr.VARIANT_KEY: {'$value': 0},
    fr.GIVENS_KEY: {},
    fr.COUNT_KEY: 1,
    fr.TYPE_KEY: fr.DECISION_TYPE,
    fr.MODEL_KEY: TEST_MODEL_NAME,
    fr.RUNNERS_UP_KEY: None,
}

TZ_AWARE_TIMESTAMP_PATTERN = '2022-01-26T14:32:11.242{}0{}:00'
TZ_NAIVE_TIMESTAMP_PATTERN = '2022-01-26T21:0{}:14.180Z'


def _get_expected_df(json_records):

    df_records = []

    for record in json_records:
        current_record_dict = deepcopy(record)
        current_record_dict[fr.TIMESTAMP_KEY] = dateutil.parser.isoparse(current_record_dict[fr.TIMESTAMP_KEY])
        current_record_dict[fr.DECISION_ID_KEY] = current_record_dict[fr.MESSAGE_ID_KEY]

        del current_record_dict[fr.MESSAGE_ID_KEY]
        del current_record_dict[fr.TYPE_KEY]
        del current_record_dict[fr.MODEL_KEY]

        df_records.append(current_record_dict)

    df = pd.DataFrame(df_records, columns=fr.DF_SCHEMA.keys())

    for column in [fr.VARIANT_KEY, fr.GIVENS_KEY]:
        df[column] = df[column].apply(lambda x: orjson.dumps(x).decode('utf-8'))

    df[fr.TIMESTAMP_KEY] = pd.to_datetime(df[fr.TIMESTAMP_KEY], utc=True)
    return df.astype(fr.DF_SCHEMA)


def test_all_tz_aware_timestamps():

    records = []
    json_records = []

    for _ in range(10):
        tz_direction = np.random.choice(['+', '-'])
        tz_int_increment = np.random.randint(0, 9)
        tz_aware_timestamp = TZ_AWARE_TIMESTAMP_PATTERN.format(tz_direction, tz_int_increment)

        current_record_dict = deepcopy(RECORD_PATTERN)
        current_record_dict[fr.TIMESTAMP_KEY] = tz_aware_timestamp
        current_record_dict[fr.MESSAGE_ID_KEY] = str(Ksuid())

        records.append(fr.FirehoseRecord(current_record_dict))
        json_records.append(current_record_dict)

    frg = fr.FirehoseRecordGroup(model_name=TEST_MODEL_NAME, records=records)
    frg_df = frg.to_pandas_df()

    expected_df = _get_expected_df(json_records=json_records)

    pd.testing.assert_frame_equal(frg_df, expected_df)


def test_all_tz_aware_identical_timestamps():
    records = []
    json_records = []

    tz_direction = '+'
    tz_int_increment = 7

    for _ in range(10):
        tz_aware_timestamp = TZ_AWARE_TIMESTAMP_PATTERN.format(tz_direction, tz_int_increment)

        current_record_dict = deepcopy(RECORD_PATTERN)
        current_record_dict[fr.TIMESTAMP_KEY] = tz_aware_timestamp
        current_record_dict[fr.MESSAGE_ID_KEY] = str(Ksuid())

        records.append(fr.FirehoseRecord(current_record_dict))
        json_records.append(current_record_dict)

    frg = fr.FirehoseRecordGroup(model_name=TEST_MODEL_NAME, records=records)
    frg_df = frg.to_pandas_df()

    expected_df = _get_expected_df(json_records=json_records)

    pd.testing.assert_frame_equal(frg_df, expected_df)


def test_mixed_tz_aware_timestamps():
    records = []
    json_records = []

    for i in range(10):

        if i < 5:
            tz_direction = np.random.choice(['+', '-'])
            tz_int_increment = np.random.randint(0, 9)
            timestamp = TZ_AWARE_TIMESTAMP_PATTERN.format(tz_direction, tz_int_increment)

        else:
            random_minutes = np.random.randint(0, 9)
            timestamp = TZ_NAIVE_TIMESTAMP_PATTERN.format(random_minutes)

        current_record_dict = deepcopy(RECORD_PATTERN)
        current_record_dict[fr.TIMESTAMP_KEY] = timestamp
        current_record_dict[fr.MESSAGE_ID_KEY] = str(Ksuid())

        records.append(fr.FirehoseRecord(current_record_dict))
        json_records.append(current_record_dict)

    frg = fr.FirehoseRecordGroup(model_name=TEST_MODEL_NAME, records=records)
    frg_df = frg.to_pandas_df()

    expected_df = _get_expected_df(json_records=json_records)

    pd.testing.assert_frame_equal(frg_df, expected_df)


def test_not_tz_aware_timestamps():
    records = []
    json_records = []

    for _ in range(10):
        random_minutes = np.random.randint(0, 9)
        timestamp = TZ_NAIVE_TIMESTAMP_PATTERN.format(random_minutes)

        current_record_dict = deepcopy(RECORD_PATTERN)
        current_record_dict[fr.TIMESTAMP_KEY] = timestamp
        current_record_dict[fr.MESSAGE_ID_KEY] = str(Ksuid())

        records.append(fr.FirehoseRecord(current_record_dict))
        json_records.append(current_record_dict)

    frg = fr.FirehoseRecordGroup(model_name=TEST_MODEL_NAME, records=records)
    frg_df = frg.to_pandas_df()

    expected_df = _get_expected_df(json_records=json_records)

    pd.testing.assert_frame_equal(frg_df, expected_df)
