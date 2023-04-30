import json
import os
import shutil

import docker
import tempfile
from pathlib import Path

from improveai import Scorer

from firehose_record import FirehoseRecordGroup, DECISION_ID_KEY
from partition import RewardedDecisionPartition, maybe_split_on_timestamp_boundaries, parquet_s3_key

# import benchmark_config as bc

MODEL_NAME = 'benchmark'
# USE_LOCAL_CHECKPOINT = False
# CACHE_CHECKPOINT = False
# CHECKPOINT_FILENAME = 'phase1.xgb'


def train_model(firehose_records, extra_hyperparameters={}, image_name: str = 'v8_trainer:latest'):
    """
    Runs a single training iteration with python docker API

    Parameters
    ----------

    Returns
    -------

    """
    with tempfile.TemporaryDirectory() as tmp_dir:

        worker_path = Path(tmp_dir)

        hyperparameters = {
            "model_name":  "benchmark",
            "max_decision_records": 1000000,
            "max_trees": 150,
            "max_features": 300,
            "max_checkpoint_age": "24 hours"
        }

        hyperparameters.update(extra_hyperparameters)

        print(json.dumps(hyperparameters))

        inputdataconfig = {}

        resourceconfig = {
            "hosts":  ["localhost"],
            "current_host": "localhost"
        }

        config_path = worker_path / 'config'
        config_path.mkdir()

        with open(config_path / 'hyperparameters.json', 'w') as f:
            f.write(json.dumps(hyperparameters))

        with open(config_path / 'inputdataconfig.json', 'w') as f:
            f.write(json.dumps(inputdataconfig))

        with open(config_path / 'resourceconfig.json', 'w') as f:
            f.write(json.dumps(resourceconfig))
            
        model_path = worker_path / 'model'
        model_path.mkdir()

        decisions_path = worker_path / 'input' / 'data' / 'decisions'
        decisions_path.mkdir(parents=True)

        write_parquet(firehose_records, decisions_path)

        # this should point to /opt/ml/checkpoints
        checkpoints_path = worker_path / 'checkpoints'
        checkpoints_path.mkdir(parents=True)

        # saved_checkpoints = os.listdir(bc.LOCAL_CHECKPOINTS_CACHE)
        #
        # if USE_LOCAL_CHECKPOINT and CHECKPOINT_FILENAME in saved_checkpoints:
        #     # copy local checkpoint to worker
        #     shutil.copy(
        #         bc.LOCAL_CHECKPOINTS_CACHE / CHECKPOINT_FILENAME,
        #         checkpoints_path / CHECKPOINT_FILENAME)

        dc = docker.from_env()
        volumes = {
            str(decisions_path): {
                "bind": '/opt/ml/input/data/decisions', "mode": 'rw'},
            str(model_path): {
                "bind": '/opt/ml/model', "mode": 'rw'},
            str(config_path): {
                "bind": '/opt/ml/input/config', "mode": 'rw'},
            # bind checkpoints volume
            str(checkpoints_path): {
                "bind": '/opt/ml/checkpoints', "mode": 'rw'},
        }

        environment = {
            'MODEL_NAME': MODEL_NAME
        }

        container = \
            dc.containers.run(
                image_name, command='train', detach=True, volumes=volumes,
                environment=environment)
        print('### Waiting for the model to train ###')
        container.wait()
        print(container.logs().decode("utf-8"))

        # # # extract trained checkpoint
        # checkpoint_objects = os.listdir(checkpoints_path)        #
        # if CACHE_CHECKPOINT and CHECKPOINT_FILENAME in checkpoint_objects:
        #     shutil.copy(
        #         checkpoints_path / CHECKPOINT_FILENAME,
        #         bc.LOCAL_CHECKPOINTS_CACHE / f'{int(len(firehose_records) / 2)}_{CHECKPOINT_FILENAME}')

        docker_cleanup()

        return Scorer(model_url=str(model_path / 'model.xgb'))


def write_parquet(firehose_records, decisions_path):

    df = FirehoseRecordGroup(MODEL_NAME, firehose_records).to_pandas_df()
    partition = RewardedDecisionPartition(MODEL_NAME, df=df)
    partition.sort()
    import time
    start = time.perf_counter()
    partition.merge()
    print(f'merge took {time.perf_counter() - start} seconds')
    print(f'Total reward: {partition.df["reward"].sum()}')

    chunks = maybe_split_on_timestamp_boundaries(partition.df)

    print(f'writing {sum(map(lambda x: x.shape[0], chunks))} rewarded decisions across {len(chunks)} partitions')
    
    # split the dataframe into multiple chunks if necessary
    for chunk in chunks:
        chunk_s3_key = parquet_s3_key(MODEL_NAME, min_decision_id=chunk[DECISION_ID_KEY].iat[0],
            max_decision_id=chunk[DECISION_ID_KEY].iat[-1], count=chunk.shape[0])
        
        chunk_path = parquet_path(decisions_path) / chunk_s3_key
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        chunk.to_parquet(chunk_path, compression='ZSTD', index=False)


def parquet_path(worker_dir):
    return worker_dir / 'input' / 'data' / 'decisions' / 'parquet'


def docker_cleanup():
    try:
        dc = docker.from_env()
        dc.containers.prune()
    except Exception as exc:
        print('While attempting to prune containers the following error occurred')
        print(exc)
