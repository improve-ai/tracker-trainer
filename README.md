# Improve AI Gym: Simple Scalable Reinforcement Learning

The Improve AI Gym is a scalable stack of serverless services that make it easy to deploy *reinforcement learning* systems in production. *Reinforcement learning* is a type of machine learning where models are trained by assigning rewards for positive outcomes. By aligning rewards with business metrics, such as revenue, conversions, or user retention, reinforcement learning automatically optimizes decisions to lift those metrics.

The stack is packaged as a virtual private cloud that you deploy on AWS. A such, all data remains private to you and there are no operational dependencies on third parties other than AWS.

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

Once subscribed, copy your the trainer Docker image URLs and paste them in **config/config.yml**

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

## Integrate a Client SDK

Improve AI SDKs are currently available for [Swift/Objective-C](https://github.com/improve-ai/ios-sdk), [Java/Kotlin](https://github.com/improve-ai/android-sdk), and [Python](https://github.com/improve-ai/python-sdk).

For example, in Swift, we would use the *trackURL* and *modelURL* that we configured in the Improve AI Gym.

```swift
import ImproveAI
```

```swift
func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {

    DecisionModel.defaultTrackURL = trackURL // trackUrl is obtained from your Improve AI Gym configuration

    DecisionModel.instances["greetings"].loadAsync(greetingsModelUrl) // greetingsModelUrl is a trained model output by the Improve AI Gym

    return true
}
```

To make a decision, use the *which* statement. *which* is like an AI if/then statement.
```swift
greeting = DecisionModel.instances["greetings"].which("Hello", "Howdy", "Hola")
```

*which* makes decisions on-device using a *decision model*. Decision models are easily trained by assigning rewards for positive outcomes.

```swift
if (success) {
    DecisionModel.instances["greetings"].addReward(1.0)
}
```

Rewards are credited to the most recent decision made by the model. *which* will make the decision that provides the highest expected reward.  When the rewards are business metrics, such as revenue or user retention, the decisions will optimize to automatically improve those metrics over time.

*That's like A/B testing on steroids.*

# Algorithm

The reinforcement learning algorithm is a [contextual multi-armed bandit](https://en.wikipedia.org/wiki/Multi-armed_bandit#Contextual_bandit) with [XGBoost](https://github.com/dmlc/xgboost) acting as the core regression algorithm. As such, it is ideal for making decisions on structured data, such as JSON or native objects in [Swift/Objective-C](https://github.com/improve-ai/ios-sdk), [Java/Kotlin](https://github.com/improve-ai/android-sdk), and [Python](https://github.com/improve-ai/python-sdk). Unlike *deep reinforcement learning* algorithms, which often requires simulator environments and hundreds of millions of decisions, this algorithm performs well with relatively modest amounts of data. Compared to A/B testing it requires exponentially less data for good results.

# Architecture

The Improve AI Gym stack consists of a number of components to track decisions and rewards, create training data, train decision models, and store the resulting models.

## Track Endpoint

The *track* HTTPS endpoint is an AWS Lambda service that scalably tracks decisions and rewards. The *track* endpoint URL is used by the client SDKs to track 
decisions and rewards.  Events received by the track endpoint Lambda are sent to AWS Firehose for persistence and further processing. 
The maximum payload size for *track* is 1MB.

## AWS Kinesis Firehose

Decision and reward data is sent from the track endpoint to AWS Kinesis Firehose. After 15 minutes or 128MB of data, the firehose data is flushed 
and written to the *firehose* S3 bucket.

## Firehose Ingest

When a new firehose file is written to the firehose S3 bucket, an AWS Lambda job ingests the new data from firehose and adds it to the training data 
for each model in the *train* S3 bucket.

## Joining Rewards with Decisions

Prior to each training, the .parquet files containing the training data are optimized and all available rewards are joined with their decisions, ensuring
the most up-to-date data is used for each training cycle.

## Decision Model Training

Using the training schedule specified in **config/config.yml** training jobs are created in AWS SageMaker.  
SageMaker will run either the FREE or PRO version of the Improve AI Trainer in a network isolated cluster. 
The resulting model will be written to the *models* S3 bucket.

## Decision Model Serving

By default, the *models* S3 bucket is private. To enable public model serving, we recommend configuring a CDN, such as AWS CloudFront, in front of the *models* S3 bucket.
