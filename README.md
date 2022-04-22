# Improve AI Gym: Track Decisions & Rewards, Train Decision Models

The Improve AI Gym is a scalable stack of serverless services that run on AWS for tracking decisions and rewards from Improve AI SDKs and managing the training of decision models.

These services are packaged into a Virtual Private Cloud that you deploy on AWS. A such, all data remains private to you and there are no operational dependencies on third parties other than AWS.

# Deployment

## Fork this repo

**For now, we suggest installing the Gym by making a private fork of this repo. We are also considering packaging the gym as an NPM package or a Serverless Framework Plugin. Please send suggestions for deployment packaging to support@improve.ai**

## Install the Serverless Framework

```console
$ npm install -g serverless
```

## Install NPM Dependencies
 
```console
$ npm install
```

## Subscribe to a Decision Model Trainer

To train models on up to 100,000 decisions, subscribe to the [free trainer](https://aws.amazon.com/marketplace/pp/prodview-pyqrpf5j6xv6g)

To train models on over 100,000 decisions, subscribe to the [pro trainer](https://aws.amazon.com/marketplace/pp/prodview-adchtrf2zyvow)

Once subscribed, copy your the trainer Docker image URLs and add them to **config/config.yml**

## Configure Models and Training Parameters

```console
$ nano config/config.yml
```

## Deploy the Stack

Deploy a new dev stage in us-east-1

```console
$ serverless deploy --stage dev
```

The output of the deployment will list the HTTPS URL for the *track* endpoint like https://xxxx.lambda-url.us-east-1.on.aws

```console
Deploying improveai-acme-demo to stage prod (us-east-1)

✔ Service deployed to stack improveai-acme-demo-prod (111s)

endpoint: https://xxx.lambda-url.us-east-1.on.aws/

```

Either configure a CDN in front of this track endpoint URL, or use it directly in the client SDKs to track decisions and rewards.

The deployment will also create a *models* S3 bucket in the form of *improveai-acme-demo-prod-models* where the continuously trained 
models will automatically be uploaded.

Either configure a CDN in front of the models S3 bucket, or make the 'models' directory public to serve models directly from S3.

# Architecture

The Improve AI Gym stack consists of a number of components to track decisions and rewards, create training data, train decision models, and store the resulting models.

## Track Endpoint

The *track* HTTPS endpoint is an AWS Lambda service that scalably tracks decisions and rewards. The *track* endpoint URL is used by the client SDKs to track 
decisions and rewards.  Events received by the track endpoint Lambda are sent to AWS Firehose for persistence and further processing. 
The maximum payload size for *track* is 1MB.

## AWS Kinesis Firehose

Decision and reward data is sent from the track endpoint to AWS Kinesis Firehose. After 15 minutes or 32MB of data, the firehose data is flushed 
and written to the *firehose* S3 bucket.

## Firehose Ingest

When a new firehose file is written to the firehose S3 bucket, an AWS Lambda job ingests the new data from firehose and adds it to the training data 
for each model in the *train* S3 bucket.

## Grooming Training Data

Prior to each training, the .parquet files containing the training data are optimized and all available rewards are joined with their decisions, ensuring
the most up-to-date data is used for each training cycle.

## Decision Model Training

Using the training schedule specified in **config/config.yml** training jobs are created in AWS SageMaker.  
SageMaker will run either the FREE or PRO version of the Improve AI Trainer in a network isolated cluster. 
The resulting model will be written to the *models* S3 bucket.

## Decision Model Serving

By default, the *models* S3 bucket is private. To enable public model serving, we recommend configuring a CDN, such as AWS CloudFront, in front of the *models* S3 bucket.
