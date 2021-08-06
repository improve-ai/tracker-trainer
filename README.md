# Improve AI Gym

# Install the Serverless Framework
npm install -g serverless

# Install the dependencies
npm install

# Subscribe to the Decision Model Trainer
Visit XXX
copy your the Docker image URL

# Edit config/config.yml
vi config/config.yml

# Deploy the service to a new dev stage in us-east-1
serverless deploy --stage dev

The output of the deployment will list the HTTPS URL for the Decision Tracker endpoint.

# Overview

The Improve AI Gym consists of three main components: decision & event tracking, reward assignment, and decision model training.

## Decision & Event Tracking

Using the *track* HTTPS endpoint, client DecisionTracker classes will send events and track decisions to the Gym.  The events received after a decision determine the total *reward*
that decision will receive.  The tracked decisions and events are sent to AWS Firehose and persisted to the Gym's firehose S3 bucket.

## Firehose Unpack

After 15 minutes or 128MB of data, the firehose is flushed to S3, the data is then ingested into EFS where it can be processed by *reward assignment* AWS batch jobs.

## Reward Assignment

By default, every 4 hours, AWS batch jobs process the new data in EFS and decisions are joined with subsequent events to determine a total reward for that decision. *rewarded decisions* 
are then uploaded to the the *train* S3 bucket and are ready to train decision models

## Model Training

The transformed training data is trained using XGBoost

## Final Model Transformation and Bundling

Models and boosted models are transformed and bundled for deployment to client libraries.
