import os
from pathlib import Path
import sys

RUN_ID = 'happy-sunday-string-variants-list-context'
RUN_CACHE = Path(f'/home/kw/Projects/upwork/trainer/benchmark/debug-runs-cache/{RUN_ID}')

LOGS_CACHE_DIR = 'trainer-logs'
CHECKPOINTS_CACHE_DIR = 'checkpoints'
DFS_CACHE_DIR = 'dfs_cache'
MODELS_CACHE_DIR = 'models_cache'

LOCAL_LOGS_CACHE = RUN_CACHE / LOGS_CACHE_DIR
LOCAL_CHECKPOINTS_CACHE = RUN_CACHE / CHECKPOINTS_CACHE_DIR
LOCAL_DFS_CACHE = RUN_CACHE / DFS_CACHE_DIR
LOCAL_MODELS_CACHE = RUN_CACHE / MODELS_CACHE_DIR


def get_ready_for_benchmark_run():

    os.environ['TRAIN_BUCKET'] = 'benchmark-train-bucket'
    os.environ['FIREHOSE_BUCKET'] = 'benchmark-firehose-bucket'

    src_abspath = os.sep.join(os.path.abspath('.').split(os.sep)[:-2])
    sys.path.append(os.sep.join([src_abspath, 'ingest']))
