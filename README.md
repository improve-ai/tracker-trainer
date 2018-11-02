# Improve.ai Reinforcement Learning Gateway for Amazon Sagemaker
Deploy a scalable reinforcement learning or multi-armed bandit api in minutes.

# reinforcement-learning-gateway
Improve.ai Reinforcement Learning Gateway

# Install the Serverless Framework
npm install -g serverless

# Install the dependencies from package.json
npm install 

# Deploy the service to a new dev stage
serverless deploy --stage dev

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
    - Hosting traffic
    - Hosting instance type
    - Type of algorithm (Multi-Armed Bandits are faster/cheaper than the XGBoost-backed Scalable Decision Service)
