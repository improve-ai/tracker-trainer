# Improve AI Gym

# Install the Serverless Framework
npm install -g serverless

# Install the dependencies from package.json
npm install

# Deploy the service to a new dev stage in us-east-1
serverless deploy --stage dev

The output of the deployment will list the HTTP URL for the Decision Tracker endpoint.

# Architecture

## Track

Data comes into the track HTTP endpoint and sent to an AWS Kinesis Firehose

## Firehose Unpack

After 15 minutes or 128MB of data, the Kinesis firehose is flushed to S3, the data is ingested and sorted by project into the /histories folder in the records bucket.

## Reward Assignment

New data from the /histories folder is processed and rewards are joined to decisions.  The result is sorted by project and model and then output to the /rewarded_decisions folder in the \*-train S3 bucket

## Model Training

The transformed training data is trained using XGBoost

## Final Model Transformation and Bundling

Models and boosted models are transformed and bundled for deployment to client libraries.
