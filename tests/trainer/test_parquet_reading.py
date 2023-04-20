import os
from pathlib import Path

from dask.distributed import Client
import numpy as np
import pandas as pd
from pytest import fixture, raises
from unittest import TestCase

import src.trainer.code.constants as tc
from src.trainer.code.parquet_io import DataFrameLoader, read_parquet_safely, \
    get_parquet_train_filters

PARQUET_READING_TESTS_DATA_DIR = \
    os.path.abspath(os.sep.join(str(__file__).split(os.sep)[:-1] + ['data', 'parquet_reading']))


def test_read_parquet_safely_bad_file():
    columns = [tc.DECISION_ID_KEY, tc.ITEM_KEY, tc.CONTEXT_KEY]
    filters = get_parquet_train_filters(loaded_columns=columns)
    path_to_bad_file = os.sep.join([PARQUET_READING_TESTS_DATA_DIR, '2021/12/21/11.parquet'])
    columns = [tc.DECISION_ID_KEY, tc.ITEM_KEY, tc.CONTEXT_KEY]
    desired_dtypes = {k: v for k, v in tc.DF_SCHEMA.items() if k in columns}
    res = \
        read_parquet_safely(parquet_path=path_to_bad_file, columns=columns, dtypes=desired_dtypes, filters=filters)
    assert isinstance(res, pd.DataFrame)
    np.testing.assert_array_equal(res.columns, columns)
    assert res.empty


def test_get_rewarded_decision_record_parquet_filters_1():
    # loaded_columns, model_evaluation: bool = False
    columns = [tc.ITEM_KEY]
    filters = get_parquet_train_filters(loaded_columns=columns)
    np.testing.assert_array_equal(filters, [[(tc.ITEM_KEY, 'not in', [None])]])


def test_get_rewarded_decision_record_parquet_filters_2():
    # loaded_columns, model_evaluation: bool = False
    columns = [tc.ITEM_KEY, tc.CONTEXT_KEY]
    filters = get_parquet_train_filters(loaded_columns=columns)
    np.testing.assert_array_equal(filters, [[(tc.ITEM_KEY, 'not in', [None]), (tc.CONTEXT_KEY, 'not in', [None])]])


def test_get_rewarded_decision_record_parquet_filters_3():
    # loaded_columns, model_evaluation: bool = False
    columns = [tc.ITEM_KEY, tc.CONTEXT_KEY, 'dummy-column']
    filters = get_parquet_train_filters(loaded_columns=columns)
    np.testing.assert_array_equal(filters, [[(tc.ITEM_KEY, 'not in', [None]), (tc.CONTEXT_KEY, 'not in', [None])]])


class TestDataLoader(TestCase):

    @fixture(autouse=True)
    def prepare_dependencies(self):
        self.client = Client()
        test_data_dir = os.getenv('TEST_DATA_DIR', None)
        assert test_data_dir is not None
        self.large_files_parquet_path = Path(os.sep.join([test_data_dir, 'parquet_reading', 'decisions']))
        self.small_files_parquet_path = Path(os.sep.join([test_data_dir, 'parquet_reading', 'small_amount_of_decisions']))

        self.default_parquet_path = None
        self.default_min_rows = 0.0
        self.default_max_rows = None
        self.default_sample = 1.0

        self.custom_min_rows = 100
        self.custom_max_rows = 1000
        self.custom_sample = 0.67

        self.columns = [tc.ITEM_KEY, tc.CONTEXT_KEY, tc.REWARD_KEY]
        yield
        self.client.close()

    def test_data_loader_constructor_only_client(self):
        # client: Client, parquet_path = None, min_rows = 0.0, max_rows = None, sample = 1.0
        loader = DataFrameLoader(client=self.client)  #, parquet_path=self.parquet_path)
        assert loader.parquet_path is self.default_parquet_path
        assert loader.min_rows == self.default_min_rows
        assert loader.max_rows is self.default_max_rows
        assert loader.sample == self.default_sample

    def test_data_loader_constructor_client_path(self):
        # client: Client, parquet_path = None, min_rows = 0.0, max_rows = None, sample = 1.0

        loader = DataFrameLoader(client=self.client, parquet_path=self.large_files_parquet_path)
        assert loader.parquet_path == self.large_files_parquet_path
        assert loader.min_rows == self.default_min_rows
        assert loader.max_rows is self.default_max_rows
        assert loader.sample == self.default_sample

    def test_data_loader_constructor_client_path_min_rows(self):
        # client: Client, parquet_path = None, min_rows = 0.0, max_rows = None, sample = 1.0
        loader = DataFrameLoader(client=self.client, parquet_path=self.large_files_parquet_path, min_rows=self.custom_min_rows)
        assert loader.parquet_path == self.large_files_parquet_path
        assert loader.min_rows == self.custom_min_rows
        assert loader.max_rows is self.default_max_rows
        assert loader.sample == self.default_sample

    def test_data_loader_constructor_client_path_min_rows_max_rows(self):
        # client: Client, parquet_path = None, min_rows = 0.0, max_rows = None, sample = 1.0
        loader = DataFrameLoader(
            client=self.client, parquet_path=self.large_files_parquet_path, min_rows=self.custom_min_rows,
            max_rows=self.custom_max_rows)
        assert loader.parquet_path == self.large_files_parquet_path
        assert loader.min_rows == self.custom_min_rows
        assert loader.max_rows is self.custom_max_rows
        assert loader.sample == self.default_sample

    def test_data_loader_constructor_client_path_min_rows_max_rows_sample(self):
        # client: Client, parquet_path = None, min_rows = 0.0, max_rows = None, sample = 1.0

        loader = DataFrameLoader(
            client=self.client, parquet_path=self.large_files_parquet_path,
            min_rows=self.custom_min_rows, max_rows=self.custom_max_rows, sample=self.custom_sample)
        assert loader.parquet_path == self.large_files_parquet_path
        assert loader.min_rows == self.custom_min_rows
        assert loader.max_rows is self.custom_max_rows
        assert loader.sample == self.custom_sample

    # TODO fix names so they contain rowcount
    def test_load_min_rows_0_max_rows_none_sample_1(self):

        loader = DataFrameLoader(client=self.client, parquet_path=self.large_files_parquet_path)
        ddf = loader.load(columns=self.columns)
        # assert that columns are as desired
        np.testing.assert_array_equal(ddf.columns, self.columns)
        print('### len(ddf) ###')
        print(len(ddf))
        assert ddf.shape[0].compute() == 14310
        # assert all variants and givens are not nullish
        assert not ddf[tc.ITEM_KEY].isnull().any().compute()
        assert not ddf[tc.CONTEXT_KEY].isnull().any().compute()

    def test_load_min_rows_250_max_rows_500_sample_1(self):
        loader = DataFrameLoader(
            client=self.client, parquet_path=self.small_files_parquet_path, min_rows=250, max_rows=500, sample=1)
        ddf = loader.load(columns=self.columns)
        np.testing.assert_array_equal(ddf.columns, self.columns)
        # only 300 records present in files
        assert ddf.shape[0].compute() == 300
        assert not ddf[tc.ITEM_KEY].isnull().any().compute()
        assert not ddf[tc.CONTEXT_KEY].isnull().any().compute()

    def test_load_min_rows_250_max_rows_500_sample_067(self):
        # test that adjusted sample modifies the input sample -> 0.67 is changed to ~0.83
        loader = DataFrameLoader(
            client=self.client, parquet_path=self.small_files_parquet_path, min_rows=250, max_rows=500, sample=0.67)
        ddf = loader.load(columns=self.columns)
        np.testing.assert_array_equal(ddf.columns, self.columns)
        # only 300 records present in files
        assert ddf.shape[0].compute() == 249
        assert not ddf[tc.ITEM_KEY].isnull().any().compute()
        assert not ddf[tc.CONTEXT_KEY].isnull().any().compute()

    def test_load_min_rows_250_max_rows_500_sample_090(self):
        # test that adjusted sample does not modify the input sample
        loader = DataFrameLoader(
            client=self.client, parquet_path=self.small_files_parquet_path, min_rows=250, max_rows=500, sample=0.90)
        ddf = loader.load(columns=self.columns)
        np.testing.assert_array_equal(ddf.columns, self.columns)
        # only 300 records present in files
        assert ddf.shape[0].compute() == 270
        assert not ddf[tc.ITEM_KEY].isnull().any().compute()
        assert not ddf[tc.CONTEXT_KEY].isnull().any().compute()
