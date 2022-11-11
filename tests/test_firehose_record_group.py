from copy import deepcopy
import orjson
import pandas as pd
from ksuid import Ksuid
import numpy as np

import firehose_record as fr


TEST_MODEL_NAME = 'test-model'
RECORD_PATTERN = {
    fr.MESSAGE_ID_KEY: None,
    fr.VARIANT_KEY: {'$value': 0},
    fr.GIVENS_KEY: {},
    fr.COUNT_KEY: 1,
    fr.TYPE_KEY: fr.DECISION_TYPE,
    fr.MODEL_KEY: TEST_MODEL_NAME,
    fr.RUNNERS_UP_KEY: None,
}

def _get_expected_df(json_records):

    df_records = []

    for record in json_records:
        current_record_dict = deepcopy(record)
        current_record_dict[fr.DECISION_ID_KEY] = current_record_dict[fr.MESSAGE_ID_KEY]

        del current_record_dict[fr.MESSAGE_ID_KEY]
        del current_record_dict[fr.TYPE_KEY]
        del current_record_dict[fr.MODEL_KEY]

        df_records.append(current_record_dict)

    df = pd.DataFrame(df_records, columns=fr.DF_SCHEMA.keys())

    for column in [fr.VARIANT_KEY, fr.GIVENS_KEY]:
        df[column] = df[column].apply(lambda x: orjson.dumps(x).decode('utf-8'))

    return df.astype(fr.DF_SCHEMA)


def test_constructor():

    records = []
    json_records = []

    for _ in range(10):
        current_record_dict = deepcopy(RECORD_PATTERN)
        current_record_dict[fr.MESSAGE_ID_KEY] = str(Ksuid())

        records.append(fr.FirehoseRecord(current_record_dict))
        json_records.append(current_record_dict)

    frg = fr.FirehoseRecordGroup(model_name=TEST_MODEL_NAME, records=records)
    assert frg.model_name == TEST_MODEL_NAME
    np.testing.assert_array_equal(frg.records, records)


def test_to_pandas_df():
    records = []
    json_records = []

    for _ in range(10):
        current_record_dict = deepcopy(RECORD_PATTERN)
        current_record_dict[fr.MESSAGE_ID_KEY] = str(Ksuid())

        records.append(fr.FirehoseRecord(current_record_dict))
        json_records.append(current_record_dict)

    frg = fr.FirehoseRecordGroup(model_name=TEST_MODEL_NAME, records=records)
    frg_df = frg.to_pandas_df()

    expected_df = _get_expected_df(json_records=json_records)

    pd.testing.assert_frame_equal(frg_df, expected_df)

