import random
from pathlib import Path

from dask.distributed import Client
import dask.dataframe as dd
from fastparquet import ParquetFile
import orjson as json
import pandas as pd
import re

from constants import CONTEXT_KEY, SAMPLE_KEY, REWARD_KEY, DF_SCHEMA, ITEM_KEY, VALID_PARQUET_FILENAME
from utils import cull_empty_df_partitions, trim_memory

FILE_SAMPLING_MIN_ROWS = 1000000
FILE_SAMPLING_MIN_FILES = 100


class DataFrameLoader:


    def __init__(self, client: Client, parquet_path = None, min_rows = 0.0, max_rows = None, sample = 1.0) -> None:

        assert not min_rows or max_rows is None or min_rows <= max_rows

        self.client = client
        self.parquet_path = parquet_path
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.sample = sample


    def load(self, columns: list):
        '''
        Load the Dask dataframe.  The list operation is performed on the master node and loading is distributed to the workers.
        For the case that there is a smallish amount of data, row-wise sampling is performed after loading an approximately sufficient
        number of parquet files. For larger amounts of data, sampling happens at the file level.

        As of 20221017 Dask doesn't support row slicing so there isn't a trivial way to trim to the max number of rows so we approximate
        '''

        print(f'selecting parquet files')

        parquet_files = []
        row_count = 0

        # list the files in reverse lexicographic order. assuming datestamped directory and file names, this will be reverse chronological order
        for path in iterate_parquet_paths_descending(self.parquet_path):

            parquet_files.append(path)
            # Add name-derived row count.
            row_count += get_parquet_file_rowcount(path)

            if self.max_rows and row_count * self.sample >= self.max_rows: # account for sampling when calculating if we have sufficient rows
                break

        # adjust the sample to accommodate the target minimum number of rows
        adjusted_sample = max(self.sample, min(self.min_rows / row_count, 1.0))

        # if we have a lot of files and rows, perform file level sampling
        if adjusted_sample < 1.0 and row_count >= FILE_SAMPLING_MIN_ROWS and len(parquet_files) >= FILE_SAMPLING_MIN_FILES:
            parquet_files = list(filter(lambda x: random.random() < adjusted_sample, parquet_files))

            # don't subsample rows because we've already sampled at the file level
            adjusted_sample = 1.0

        print(f'parquet files count: {len(parquet_files)}, min: {str(parquet_files[-1])}, max: {str(parquet_files[0])}')

        return load_dataframe(
            self.client, parquet_files=parquet_files, columns=columns, sample=adjusted_sample)


def iterate_parquet_paths_descending(p: Path):
    # implemented as an iterator to ensure minimal list operations
    for sub in sorted(p.iterdir(), reverse=True):
        # descend subdirectories first
        if sub.is_dir():
            yield from iterate_parquet_paths_descending(sub)
        elif re.search('\.parquet$', str(sub)):
            yield sub


def get_parquet_file_rowcount(parquet_path: Path):
    # use the file name to determine the row count
    if re.search(VALID_PARQUET_FILENAME, parquet_path.name):
        return int(parquet_path.name.split('-')[2])

    # fall back to actually opening the parquet file.
    print(f'unable to determine row count from file name, loading: {parquet_path}')
    try:
        return ParquetFile(parquet_path).count()
    except Exception as e:
        print(f'error loading parquet: {parquet_path}, exception: {e}')
        return 0


def load_dataframe(client: Client, parquet_files, columns: list, sample = 1.0):

    print(f'loading parquet files')

    # subset dtypes
    desired_dtypes = {column: DF_SCHEMA[column] for column in columns}

    workers = list(client.scheduler_info()['workers'].keys())

    # distribute filenames reading futures across workers
    ddf_futures = [
        client.submit(
            read_parquet_safely, parquet_path=str(parquet_path), columns=columns, dtypes=desired_dtypes,
            filters=get_parquet_train_filters(loaded_columns=columns),
            workers=workers, fifo_timeout='0 ms')
        for parquet_path in parquet_files]

    meta_df = pd.DataFrame(columns=list(desired_dtypes.keys())).astype(desired_dtypes)
    ddf = dd.from_delayed(ddf_futures, meta=meta_df)

    # subsample rows
    if sample < 1.0:
        ddf = ddf.sample(frac=sample)

    ddf = cull_empty_df_partitions(ddf)

    if REWARD_KEY in ddf.columns:
        ddf[REWARD_KEY] = ddf[REWARD_KEY].fillna(0.0)

    _decode_json(ddf, columns)

    decision_count = ddf.shape[0].compute()
    print(f'decisions: {decision_count}, partitions: {ddf.npartitions}, decisions/partitions: {decision_count/ddf.npartitions}')

    client.run(trim_memory)

    return ddf


def read_parquet_safely(parquet_path, columns: list, dtypes: dict, filters: list) -> pd.DataFrame:
    """
    Reads a single parquet file with a try-catch clause returning an empty DF if unable to load.

    Parameters
    ----------
    parquet_path: str
        path to loaded parquet file
    columns: list
        list of columns to be loaded from parquet
    dtypes: dict
        dtypes to be forced on loaded DF

    Returns
    -------
    pd.DataFrame
        pandas DF from parquet file

    """

    try:
        return pd.read_parquet(parquet_path, columns=columns, filters=filters, row_filter=True).astype(dtypes)
    except Exception as e:
        print(f'error loading parquet, skipping: {parquet_path}, exception: {e}')
        # As of 2022-02, 107 error is caused by reading a file after it is deleted on S3 and is an unrecoverable condition
        if e.args[0] == 107:
            raise Exception(f'Fatal 107 error encountered, FastFile mode file system disconnected. Report issue to AWS Support re: SageMaker & FastFile mode. If issue persists, try File mode. exception: {e}')
            
        # return empty dataframe
        return pd.DataFrame(columns=columns).astype(dtypes)


def get_parquet_train_filters(loaded_columns):
    """
    Filters out partial rewarded decision records, that is rows where either 'variant' or 'givens' is not set.
    These filtered out records correspond to orphaned rewards.

    Parameters
    ----------
    loaded_columns: list
        list of columns present in a dd.DataFrame

    Returns
    -------
    list
        list of tuples with filtering conditions

    """

    assert ITEM_KEY in loaded_columns or CONTEXT_KEY in loaded_columns

    filter_tuples = [(ITEM_KEY, 'not in', [None]), (CONTEXT_KEY, 'not in', [None])]

    return [[filter_tuple for filter_tuple in filter_tuples if filter_tuple[0] in loaded_columns]]

    
def _decode_json(df, columns):

    expected_meta = pd.Series(dtype='O')

    for key in [ITEM_KEY, CONTEXT_KEY, SAMPLE_KEY]:
        if key in columns:
            df[key] = df[key].map(lambda x: None if x is None else json.loads(x), meta=expected_meta)


