import numpy as np
import os
import orjson

import pandas as pd
from pytest import raises

import src.trainer.code.constants as tc
from src.trainer.code.feature_encoder import FeatureEncoder
from src.trainer.code.propensities import encode_for_train, encode_partition, \
    encode_item
from src.trainer.code.feature_flattener import flatten_context


# test encode_variant
def _get_feature_encoder(
        complete_test_case: dict,
        model_seed_key: str = 'model_seed') -> FeatureEncoder:
    model_seed = complete_test_case.get(model_seed_key, None)
    if not isinstance(model_seed, int):
        raise TypeError(
            '`model_seed` is of a wrong type - {} instead of int'
            .format(type(model_seed)))

    feature_encoder = FeatureEncoder(model_seed=model_seed)
    return feature_encoder


def _read_test_case(test_case_filename: str):

    test_data_dir = os.getenv('TEST_CASES_DIR')
    test_case_path = \
        os.sep.join(
            [test_data_dir, 'trainer_test_cases', test_case_filename])

    with open(test_case_path, 'r') as test_case_file:
        complete_test_case = orjson.loads(test_case_file.read())
    return complete_test_case


def _test_encode_item_against_json(
        test_case_filename: str, test_case_key: str = 'test_case',
        item_key: str = 'item', context_key: str = 'context',
        kwargs_key: str = 'encode_item_kwargs',
        output_key: str = 'test_output'):

    complete_test_case = _read_test_case(test_case_filename=test_case_filename)

    test_case_input = complete_test_case.get(test_case_key, None)

    if test_case_input is None:
        raise ValueError('test case input is missing')

    input_item = test_case_input.get(item_key, None)
    input_context = test_case_input.get(context_key, None)

    encode_item_kwargs = test_case_input.get(kwargs_key, None)

    # feature_encoder = _get_feature_encoder(complete_test_case=complete_test_case)
    context_features = flatten_context(context=input_context)

    # context_features, item, unix_timestamp, chosen=True, weight=1.0
    calculated_result = \
        encode_item(context_features=context_features, item=input_item,
                    **encode_item_kwargs)

    expected_result = complete_test_case.get(output_key, None)
    if expected_result is None:
        raise \
            ValueError(
                'Bad expected output for test file: {}'.format(test_case_filename))

    assert calculated_result == expected_result


def _test_encode_for_train_against_json(
        test_case_filename: str, test_case_key: str = 'test_case',
        record_key: str = 'record', output_key: str = 'test_output'):

    complete_test_case = _read_test_case(test_case_filename=test_case_filename)

    test_case_input = complete_test_case.get(test_case_key, None)

    if test_case_input is None:
        raise ValueError('test case input is missing')

    input_record = test_case_input.get(record_key, None)

    # encode_for_train(decision_id, item, context, sample, count)
    calculated_result = \
        encode_for_train(
            input_record['decision_id'], input_record['item'], input_record['context'],
            input_record['sample'], input_record['count'])

    print('### calculated_result ###')
    print(calculated_result)

    expected_result = complete_test_case.get(output_key, None)

    print('### expected_result ###')
    print(expected_result)

    if expected_result is None:
        raise \
            ValueError(
                'Bad expected output for test file: {}'.format(test_case_filename))

    assert calculated_result == expected_result


def test_encode_item_empty_item_empty_context_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_EMPTY_ITEM_EMPTY_CONTEXT_CHOSEN'))


def test_encode_item_empty_item_nonempty_context_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_EMPTY_ITEM_NONEMPTY_CONTEXT_CHOSEN'))


def test_encode_item_none_item_empty_context_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_NONE_ITEM_EMPTY_CONTEXT_CHOSEN'))


def test_encode_item_none_item_nonempty_context_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_NONE_ITEM_NONEMPTY_CONTEXT_CHOSEN'))


def test_encode_item_nonempty_item_empty_context_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_NONEMPTY_ITEM_EMPTY_CONTEXT_CHOSEN'))


def test_encode_item_nonempty_item_nonempty_context_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_NONEMPTY_ITEM_NONEMPTY_CONTEXT_CHOSEN'))


def test_encode_item_nonempty_item_nonempty_context_not_chosen():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_NONEMPTY_ITEM_NONEMPTY_CONTEXT_NOT_CHOSEN'))


def test_encode_item_zero_timestamp():
    _test_encode_item_against_json(
        test_case_filename=os.getenv(
            'ENCODE_ITEM_ZERO_TIMESTAMP'))


def test_encode_item_raises_for_negative_timestamp():
    complete_test_case = \
        _read_test_case(
            test_case_filename=os.getenv(
                'ENCODE_ITEM_RAISES_FOR_NEGATIVE_TIMESTAMP'))

    test_case_input = complete_test_case.get('test_case')
    context_features = test_case_input.get('context')
    input_item = test_case_input.get('item')
    encode_item_kwargs = test_case_input.get('encode_item_kwargs')

    with raises(AssertionError) as aerr:
        encode_item(context_features=context_features, item=input_item, **encode_item_kwargs)

        assert 'AssertionError' in str(aerr.value)


def test_encode_item_raises_for_non_positive_weight():
    complete_test_case = \
        _read_test_case(
            test_case_filename=os.getenv(
                'ENCODE_ITEM_RAISES_FOR_NON_POSITIVE_WEIGHT'))

    test_case_input = complete_test_case.get('test_case')
    context_features = test_case_input.get('context')
    input_item = test_case_input.get('item')
    encode_item_kwargs = test_case_input.get('encode_item_kwargs')

    with raises(AssertionError) as aerr:
        encode_item(context_features=context_features, item=input_item, **encode_item_kwargs)

        assert 'AssertionError' in str(aerr.value)


def test_encode_for_train_empty_context_no_sample():
    _test_encode_for_train_against_json(
        os.getenv(
            'ENCODE_FOR_TRAIN_EMPTY_CONTEXT_NO_SAMPLE'))


def test_encode_for_train_empty_item_no_context_no_sample():
    _test_encode_for_train_against_json(
        os.getenv(
            'ENCODE_FOR_TRAIN_EMPTY_ITEM_NO_CONTEXT_NO_SAMPLE'))


def test_encode_for_train_full():
    _test_encode_for_train_against_json(
        os.getenv('ENCODE_FOR_TRAIN_FULL'))


def test_encode_for_train_null_sample():
    _test_encode_for_train_against_json(
        os.getenv('ENCODE_FOR_TRAIN_NULL_SAMPLE'))


# TODO test encode_partition
def _test_encode_partition_against_json(
        test_case_filename: str, test_case_key: str = 'test_case',
        records_key: str = 'records', output_key: str = 'test_output'):

    complete_test_case = _read_test_case(test_case_filename=test_case_filename)

    test_case_input = complete_test_case.get(test_case_key, None)

    if test_case_input is None:
        raise ValueError('test case input is missing')

    input_records = test_case_input.get(records_key, None)
    input_df = pd.DataFrame(input_records)

    # count, runners_up
    encoded_partition = encode_partition(df=input_df)

    expected_output = complete_test_case[output_key]

    for cel, eel in zip(encoded_partition, expected_output):
        print('#####')
        print(cel)
        print(eel)

    np.testing.assert_array_equal(encoded_partition, expected_output)


def test_encode_partition_empty_context_no_sample():
    _test_encode_partition_against_json(
        test_case_filename=os.getenv('ENCODE_PARTITION_EMPTY_CONTEXT_NO_SAMPLE'))


def test_encode_partition_none_sample():
    _test_encode_partition_against_json(
        test_case_filename=os.getenv('ENCODE_PARTITION_NONE_SAMPLE'))


def test_encode_partition_none_item():
    _test_encode_partition_against_json(
        test_case_filename=os.getenv('ENCODE_PARTITION_NONE_ITEM'))


def test_encode_partition_no_sample():
    _test_encode_partition_against_json(
        test_case_filename=os.getenv('ENCODE_PARTITION_NO_SAMPLE'))


def test_encode_partition():
    _test_encode_partition_against_json(
        test_case_filename=os.getenv('ENCODE_PARTITION'))


def test_encode_partition_empty_item_no_context_no_sample():
    _test_encode_partition_against_json(
        test_case_filename=os.getenv('ENCODE_PARTITION_EMPTY_ITEM_NO_CONTEXT_NO_SAMPLE'))

