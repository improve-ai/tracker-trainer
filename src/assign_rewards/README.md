
## About the tests

The testing framework is `pytest`, all its configuration can be located in the files `conftest.py` and `pytest.ini`.

The tests can be run both locally and in Docker. 

To run them locally:
```
pytest -vs
```

To run them in Docker:
```
sudo docker image build -f ./Docker/Dockerfile -t awsbatch/worker_image .
sudo docker run awsbatch/worker_image pytest -vs
```

## Notes:
- Handling of SIGTERM: it checks between each record processing if a SIGTERM signal has been received, and quits.
- Notice that in the Dockerfile currently there is one RUN of a script that creates some fake data (to be able to have a running job for some minutes, so that a manual job cancellation can be tried). This can be deleted after is  of no more use.
- There are logs with various levels of information. The current level is set at INFO (DEBUG produces way too many messages), maybe they are still too many messages?
- The script has one `config.py` file with some constants.
- Notice the parameter "BidPercentage" in the serverless template, it will determine all the Spot cost behavior. Currently I set it at 50% of the original instances price.
- There are some configuration variables in the serverless template.
- The script worker depends on various environent variables. The lambda script sets them and launches the job correctly. If you want to launch the worker in Docker or locally, you need to set these variables beforehand. If that would be the case, of testing locally or in Docker, you would also like to mount a volume.

You may do something like this:
```
(Use export if in Linux)
set DEFAULT_REWARD_WINDOW_IN_SECONDS=7200
set AWS_BATCH_JOB_ARRAY_INDEX=0
set JOIN_REWARDS_JOB_ARRAY_SIZE=3
set JOIN_REWARDS_REPROCESS_ALL=True
set PATH_TOWARDS_EFS=./src
python worker.py
```

```
sudo docker create \
-it \
--name worker_container \
--env AWS_BATCH_JOB_ARRAY_INDEX=2 \
--env JOIN_REWARDS_JOB_ARRAY_SIZE=3 \
--env JOIN_REWARDS_REPROCESS_ALL=False \
--env DEFAULT_REWARD_WINDOW_IN_SECONDS=7200 \
--env PATH_TOWARDS_EFS=/mnt/efs \
--volume="/home/user/efs:/mnt/efs" \
worker_image

sudo docker container start --interactive worker_container
```