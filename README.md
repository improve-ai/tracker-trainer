# Improve AI Tracker/Trainer - Track Rewards & Train Ranking Models

The Tracker/Trainer is a stack of serverless components that run on AWS to cheaply and easily track JSON items and their rewards from Improve AI libraries. These rewards are joined with the tracked items that they're associated with and used as input to training new scoring and ranking models.

# Deployment

## Fork this repo

Make a private fork of this repo. This way your model configuration is stored in revision control.

## Install the Serverless Framework

```console
$ npm install -g serverless
```

## Install NPM Dependencies
 
```console
$ npm install
```

## Configure Models and Training Parameters

```console
$ nano config/config.yml
```

## Deploy the Stack

Deploy a new dev stage in us-east-1

```console
$ serverless deploy --stage dev
```

```console
Deploying improveai-acme-demo to stage dev (us-east-1)

✔ Service deployed to stack improveai-acme-demo-dev (111s)

endpoint: https://xxxx.lambda-url.us-east-1.on.aws/

```

The output of the deployment will list the *track endpoint* URL like **https://xxxx.lambda-url.us-east-1.on.aws**.  The *track endpoint* URL may be used directly by the client SDKs to track decisions and rewards.  Alternately, a CDN may be configured in front of the *track endpoint* URL for greater administrative control.

The deployment will also create a *models* S3 bucket in the form of *improveai-{organization}-{project}-{stage}-models*. After each round of training, updated models are automatically uploaded to the *models* bucket.

The *models* bucket is private by default.  Make the '/models/latest/' directory public to serve models directly from S3. Alternatively, a CDN may be configured in front of the models S3 bucket.

Model URLs follow the template of **https://{modelsBucket}.s3.amazonaws.com/models/latest/{modelName}.{mlmodel|xgb}.gz**. The Android and Python SDKs use .xgb.gz models and the iOS SDK uses .mlmodel.gz models.

## Integrate a Ranker Library

Improve AI libraries are currently available for [Swift](https://github.com/improve-ai/ios-sdk), [Java](https://github.com/improve-ai/android-sdk), and [Python](https://github.com/improve-ai/python-sdk).

# Algorithm

The reinforcement learning algorithm is a [contextual multi-armed bandit](https://en.wikipedia.org/wiki/Multi-armed_bandit#Contextual_bandit) with [XGBoost](https://github.com/dmlc/xgboost) acting as the core regression algorithm. As such, it is ideal for making decisions on structured data, such as JSON or native objects in [Swift/Objective-C](https://github.com/improve-ai/ios-sdk), [Java/Kotlin](https://github.com/improve-ai/android-sdk), and [Python](https://github.com/improve-ai/python-sdk). Unlike *deep reinforcement learning* algorithms, which often require simulator environments and hundreds of millions of decisions, this algorithm performs well with the more modest amounts of data found in real world applications. Compared to A/B testing it requires exponentially less data for good results.

# License

The Tracker/Trainer is available under the Sustainable Use License. It is free to use for internal business purposes and personal use, though cloud providers are restricted from commerializing the APIs. For custom licensing contact support@improve.ai.

# Integration Assistance

For professional integration services, please contact [Konrad Wiecko](mailto:konrad.wiecko@gmail.com).
