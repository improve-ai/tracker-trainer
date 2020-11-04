
## Instructions
Once all the stack is deployed, follow these instructions to upload a Docker image to ECR and launch a test job.

### 1. Build the image
```
sudo docker image build -f ./Docker/Dockerfile -t awsbatch/worker_image .
```

### 1. Login into ECR
```
aws ecr get-login --no-include-email --region us-west-2
```

The above step will return a docker login command, use it, so you are authenticated in ECR.


### 2. Tag the image
To push the image to ECR, it needs to be tagged with the following structure:
 `amazon_erc_registry/repository_name:image_tag`.

The `amazon_ecr_registry` is:

    AWS_ACCOUNT_ID.dkr.ecr.AWS_REGION.amazonaws.com

The `repository_name` has been defined in the `serverless.yml` template:

    batch-processing-job-repository-${opt:stage, self:provider.stage}

The `image_tag` is optional. If undefined, it's assumed to be:

    latest

Example:
```
222466135929.dkr.ecr.us-west-2.amazonaws.com/batch-processing-job-repository-dev:latest
```
And use it when tagging the image
```
sudo docker tag awsbatch/worker_image BIG_CRAFTED_TAG
```
More information in the AWS documentation: [Pushing an image to ECR](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html)
### 3. Push the image to ECR:
```
sudo docker push CRAFTED_ECR_DESTINATION
```

### 4. Launch a job:
Locate the lambda function named "improve-v5-dev-joinRewards" and run it, it should launch an AWS Batch job.


## About the tests

The testing framework is `pytest`, all its configuration can be located in the files `conftest.py` and `pytest.ini`.

The tests can be run both locally and in Docker. 

To run them locally:
```
pytest -vs
```

To run them in Docker:
```
sudo docker image build -t awsbatch/worker_image .
sudo docker run awsbatch/worker_image pytest
```

## Notes:
- Handling of SIGTERM: it checks between each record processing if a SIGTERM signal has been received, and quits.
- Notice that in the Dockerfile currently there is one RUN of a script that creates some fake data (to be able to have a running job for some minutes, so that a manual job cancellation can be tried). This can be deleted after is  of no more use.
- There are logs with various levels of information. The current level is set at INFO (DEBUG produces way too many messages), maybe they are still too many messages?
- The script has one `config.py` file with some constants.

- I've put some configuration variables in the serverless template.
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
--env PATH_TOWARDS_EFS=/efs \
--volume="/home/user/efs:/efs" \
worker_image

sudo docker container start --interactive worker_container
```