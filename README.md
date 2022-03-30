# Improve AI Gym: Track Decisions & Rewards, Train Decision Models

The Improve AI Gym is a set of services that run on AWS for tracking decisions and rewards from Improve AI client SDKs and managing the training of decision models.

These services are packaged into a Virtual Private Cloud that you deploy on AWS.  A such, all data remains private to you and there are no dependencies on third parties other than AWS.

# Deployment

## Fork this repo

**For now, we suggest installing the Gym by forking this repo. We are also considering packaging the gym as an NPM package or a Serverless Framework Plugin. Please send suggestions for deployment packaging to support@improve.ai**

## Install the Serverless Framework

```console
$ npm install -g serverless
```

## Install the dependencies
 
```console
$ npm install
```

## Subscribe to a Decision Model Trainer

To train models on up to 100,000 decisions, subscribe to the free trainer at XXX

To train models on over 100,000 decisions, subscribe to the pro trainer at XXX

Copy your the Docker image URLs and add them to config/config.yml

## Configure Models and Training Parameters

```console
$ nano config/config.yml
```

## Deploy the Stack

to a new dev stage in us-east-1

```console
$ serverless deploy --stage dev
```

The output of the deployment will list the HTTPS URL for the Decision Tracker endpoint like https://xxxxxxxx.execute-api.us-east-1.amazonaws.com/track

Using the AWS API Gateway console, create a custom, stable DNS mapping to the track endpoint.

Either configure a CDN in front of the S3 models bucket, or make the 'models' directory public to serve models directly from S3.

# VPC Architecture

The Improve AI Gym stack consists of a number of components to track decisions and rewards, create training data, train decision models, and store the resulting models.

## Track Endpoint

AWS API Gateway and AWS Lambda are used to provide the *track* HTTPS endpoint. The *track* endpoint URL is used by the client SDKs to track decisions and rewards. Events received by the track endpoint Lambda are sent to AWS Firehose for persistence and further processing.

## AWS Kinesis Firehose

Decision and reward data is sent from the track endpoint to AWS Kinesis Firehose. After 15 minutes or 32MB of data, the firehose data is flushed and written to the *firehose* S3 bucket.

## Reward Assignment

When a new firehose file is written to the firehose S3 bucket, an AWS Batch job is created to ingest the new data from firehose and update the training data in the *train* S3 bucket.

## Decision Model Training

Using the training schedule specified in **config/config.yml** training jobs are created in AWS SageMaker.  SageMaker will run either the FREE or PRO version of the Improve AI Trainer in a network isolated cluster. The resulting model will be written to the *models* S3 bucket.

## Model Serving

By default, the *models* S3 bucket is private. To enable public model serving, we recommend configuring a CDN, such as AWS CloudFront in front of the *models* S3 bucket.
