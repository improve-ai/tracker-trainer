# Improve.ai Decision Service Gateway

# Install the Serverless Framework
npm install -g serverless

# Install the dependencies from package.json
npm install 

# Deploy the s3 buckets and other service resources to us-west-2
cd resources ; npm i serverless-pseudo-parameters; serverless deploy --stage dev ; cd ..

# Deploy the service to a new dev stage in us-west-2
serverless deploy --stage dev

The output of the deployment will list the lambda endpoints for /choose and /track.

Once the deployment is complete, you'll need to create a Usage Plan and API keys using AWS API Gateway.  You can also specify API keys in the serverless.yml, but this is not recommended for production.

# Training Pipeline

## Track

Data comes into the track HTTP endpoint and sent to an AWS Kinesis Firehose

## Firehose Unpack

After 15 minutes or 128MB of data, the Kinesis firehose is flushed to S3, the data is ingested and sorted by project into the /histories folder in the records bucket.

## Reward Assignment

New data from the /histories folder is processed and rewards are joined to decisions.  The result is output to the /rewarded_decisions folder in the records bucket.

## Model Training

A Sagemaker training job ingests the data from /rewarded_decisions and trains a decision model.

## XGBoost Feature Transformation

A Sagemaker batch transform job transforms data from /rewarded_decisions into a format suitable for training a model using XGBoost.

## XGBoost Model Training

The transformed training data is trained using XGBoost

## Final Model Transformation and Bundling

Models and boosted models are transformed and bundled for deployment to client libraries.

# Reducing Training Delay
Factors that influence the time it takes new data to be deployed to a live model:
    - Firehose buffering configuration (default: 15 minutes)
    - Training frequency (default: every 15 minutes)
    - The total size of the training data set
    - The CPU performance of the training EC2 instance

# Controlling Costs
The simplest way to control costs is to tune your training frequency.  Simply edit the schedule in dispatchTrainingJobs: in the serverless.yml.

Other factors that influence costs include:
    - Total training time
    - Training instance type
