'use strict';

const AWS = require('aws-sdk');
const dateFormat = require('date-format');
const uuidv4 = require('uuid/v4');
const shajs = require('sha.js')
const _ = require('lodash');

const s3 = new AWS.S3();
const sagemaker = new AWS.SageMaker({ maxRetries: 100, retryDelayOptions: { customBackoff: sagemakerBackoff }});

const naming = require("./naming.js")
const customize = require("./customize.js")

module.exports.dispatchTrainingJobs = async () => {

  console.log(`dispatching training jobs`)
  return Promise.all(Object.entries(naming.getModelsByProject()).map(([projectName, models]) => 
    Promise.all(models.map(model => 
      createFeatureTrainingJob(projectName, model)))))
}

function createFeatureTrainingJob(projectName, model) {
  
  let hyperparameters = {} 
  if (customize.config.binaryRewards) {
    Object.assign(hyperparameters,  { binary_rewards: "true" })
  }
  
  const trainingJobName = getFeatureTrainingJobName(projectName, model)
  
  console.log(`creating feature training job ${trainingJobName} project ${projectName} model ${model}`)
  
  var params = {
    TrainingJobName: trainingJobName,
    HyperParameters: hyperparameters,
    AlgorithmSpecification: { /* required */
      TrainingImage: process.env.FEATURE_TRAINING_IMAGE,
      TrainingInputMode: "Pipe",
    },
    InputDataConfig: [ 
      {
        ChannelName: 'train',
        CompressionType: 'Gzip',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: naming.getRewardedActionTrainS3Uri(projectName, model),
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
      {
        ChannelName: 'validation',
        CompressionType: 'Gzip',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: naming.getRewardedActionValidationS3Uri(projectName, model), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
    ],
    OutputDataConfig: { 
      S3OutputPath: naming.getFeatureModelsS3Uri(projectName, model), 
    },
    ResourceConfig: { 
      InstanceCount: process.env.FEATURE_TRAINING_INSTANCE_COUNT, 
      InstanceType: process.env.FEATURE_TRAINING_INSTANCE_TYPE,
      VolumeSizeInGB: process.env.FEATURE_TRAINING_VOLUME_SIZE_IN_GB
    },
    RoleArn: process.env.FEATURE_TRAINING_ROLE_ARN,
    StoppingCondition: { 
      MaxRuntimeInSeconds: process.env.FEATURE_TRAINING_MAX_RUNTIME_IN_SECONDS,
    },
  };

  return sagemaker.createTrainingJob(params).promise().catch(error => {
    // handle the error locally because the training job should not be re-attempted.
    // the most likely cause of failure is a configured model that is not yet receiving events
    console.log(`error creating feature training job ${trainingJobName} project ${projectName} model ${model}`, error)
  })
}

module.exports.featureModelCreated = async (event, context) => {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  if (!event.Records || !event.Records.length > 0 || event.Records.some(record => !record.s3 || !record.s3.bucket || !record.s3.bucket.name || !record.s3.object || !record.s3.object.key)) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }
  
  // s3 only ever includes one record per event, but this spec allows multiple, so multiple we will process.
  return Promise.all(event.Records.map(s3EventRecord => {
    const s3Key = s3EventRecord.s3.object.key

    // feature_models/projectName/modelName/<feature training job>/model.tar.gz
    let [projectName, model, trainingJobName] = s3Key.split('/').slice(1,4)
    trainingJobName = "t"+trainingJobName.substring(1) // TODO not right???

    // Use the trainingJobName as the ModelName
    let params = {
      ExecutionRoleArn: process.env.FEATURE_TRAINING_ROLE_ARN,
      ModelName: trainingJobName,
      PrimaryContainer: { 
        Image: process.env.FEATURE_TRAINING_IMAGE,
        ModelDataUrl: `s3://${s3EventRecord.s3.bucket.name}/${s3Key}`,
      }
    }

    console.log(`Attempting to create model ${trainingJobName}`);
    return sagemaker.createModel(params).promise().then((response) => {
      if (response.ModelArn) {
        // model created, kick off the transform job
        return createTransformJob(projectName, model, trainingJobName) // trainingJobName is the ModelName
      } else {
        throw new Error("No ModelArn in response, assuming failure");
      }
    })
  }))
}

function createTransformJob(projectName, model, trainingJobName) {
  
  var params = {
    TransformJobName: trainingJobName,
    ModelName: trainingJobName,
    TransformInput: {
      CompressionType: 'Gzip',
      DataSource: { 
        S3DataSource: { 
          S3DataType: "S3Prefix",
          S3Uri: naming.getRewardedActionS3Uri(projectName, model), // transform all train/validation splits. XGBoost will seperate them again.
        }
      },
      SplitType: "Line",
    },
    TransformOutput: { 
      AssembleWith: "None",
      S3OutputPath: naming.getTransformedS3Uri(projectName, model),
    },
    TransformResources: { 
      InstanceType: process.env.TRANSFORM_INSTANCE_TYPE,
      InstanceCount: process.env.TRANSFORM_INSTANCE_COUNT, 
    },
  };
  
  console.log(`Attempting to create transform job ${trainingJobName}`);
  return sagemaker.createTransformJob(params).promise()
}

module.exports.transformJobCompleted = async function(event, context) {
  console.log(`processing cloudwatch event ${JSON.stringify(event)}`)

  if (!event.detail || !event.detail.TransformJobName || event.detail.TransformJobStatus !== "Completed" ||
      !event.detail.TransformOutput || !event.detail.TransformOutput.S3OutputPath) {
    throw new Error(`WARN: Invalid cloudwatch event ${JSON.stringify(event)}`)
  }
  
  // TODO is this a record transform job or model transform job?
  
  const transformJobName = event.detail.TransformJobName
  const [projectName, model] = getProjectNameAndModelFromTransformS3OutputPath(event.detail.TransformOutput.S3OutputPath)
  
  // change the name from -f to -x
  const trainingJobName = transformJobName.substring(0, transformJobName.length-1)+'x'
  
  return createXGBoostTrainingJob(projectName, model, trainingJobName)
}

function createXGBoostTrainingJob(projectName, model, trainingJobName) {
  
  console.log(`creating xgboost training job ${trainingJobName} project ${projectName} model ${model}`)
  
  var params = {
    TrainingJobName: trainingJobName,
    HyperParameters: naming.getXGBoostHyperparameters(projectName, model),
    AlgorithmSpecification: { /* required */
      TrainingImage: process.env.XGBOOST_TRAINING_IMAGE,
      TrainingInputMode: "File",
    },
    InputDataConfig: [ 
      {
        ChannelName: 'train',
        CompressionType: 'None',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: naming.getTransformedTrainS3Uri(projectName, model), 
            S3DataDistributionType: "ShardedByS3Key",
          }
        },
      },
      {
        ChannelName: 'validation',
        CompressionType: 'None',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: naming.getTransformedValidationS3Uri(projectName, model), 
            S3DataDistributionType: "ShardedByS3Key",
          }
        },
      },
    ],
    OutputDataConfig: { 
      S3OutputPath: naming.getXGBoostModelsS3Uri(projectName, model), 
    },
    ResourceConfig: { 
      InstanceCount: process.env.XGBOOST_TRAINING_INSTANCE_COUNT, 
      InstanceType: process.env.XGBOOST_TRAINING_INSTANCE_TYPE,
      VolumeSizeInGB: process.env.XGBOOST_TRAINING_VOLUME_SIZE_IN_GB
    },
    RoleArn: process.env.FEATURE_TRAINING_ROLE_ARN,
    StoppingCondition: { 
      MaxRuntimeInSeconds: process.env.XGBOOST_TRAINING_MAX_RUNTIME_IN_SECONDS,
    },
  };

  return sagemaker.createTrainingJob(params).promise()
}

module.exports.xgboostModelCreated = async (event, context) => {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  if (!event.Records || !event.Records.length > 0 || event.Records.some(record => !record.s3 || !record.s3.bucket || !record.s3.bucket.name || !record.s3.object || !record.s3.object.key)) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }
  
  // s3 only ever includes one record per event, but this spec allows multiple, so multiple we will process.
  return Promise.all(event.Records.map(s3EventRecord => {
    const s3Key = s3EventRecord.s3.object.key

    // feature_models/projectName/modelName/<feature training job>/model.tar.gz
    let [projectName, model, trainingJobName] = s3Key.split('/').slice(1,4)

    // Use the trainingJobName as the ModelName
    let params = {
      ExecutionRoleArn: process.env.FEATURE_TRAINING_ROLE_ARN,
      ModelName: trainingJobName,
      PrimaryContainer: { 
        Image: process.env.FEATURE_TRAINING_IMAGE,
        ModelDataUrl: `s3://${s3EventRecord.s3.bucket.name}/${s3Key}`,
      }
    }

    // TODO should I load the XGBoost model as a model into the transform job or should
    // I just use the original feature model?  I think I need to use the feature model and input
    // the XGBoost model because the XGBoost model will only have xgboost info in it.
    // the model I think will be called xgboost-model
    
    
    console.log(`Attempting to create model ${trainingJobName}`);
    return sagemaker.createModel(params).promise().then((response) => {
      if (response.ModelArn) {
        return [projectName, model, trainingJobName]; // trainingJobName is the ModelName
      } else {
        throw new Error("No ModelArn in response, assuming failure");
      }
    }).then(([projectName, model, trainingJobName]) => {
      trainingJobName = "t"+trainingJobName.substring(1)
      return ""//createTransformJob(projectName, model, trainingJobName)
    })
  }))
}


function createModelTransformJob(projectName, model, trainingJobName) {
  
  var params = {
    TransformJobName: trainingJobName,
    ModelName: trainingJobName,
    TransformInput: {
      CompressionType: 'Gzip',
      DataSource: { 
        S3DataSource: { 
          S3DataType: "S3Prefix",
          S3Uri: naming.getRewardedActionS3Uri(projectName, model), // transform all train/validation splits. XGBoost will seperate them again.
        }
      },
      SplitType: "Line",
    },
    TransformOutput: { 
      AssembleWith: "None",
      S3OutputPath: naming.getTransformedS3Uri(projectName, model),
    },
    TransformResources: { 
      InstanceType: process.env.TRANSFORM_INSTANCE_TYPE,
      InstanceCount: process.env.TRANSFORM_INSTANCE_COUNT, 
    },
  };
  
  console.log(`Attempting to create transform job ${trainingJobName}`);
  return sagemaker.createTransformJob(params).promise()
}

function getFeatureTrainingJobName(projectName, model) {
  
  // every single training job must have a unique name per AWS account
  return `${getAlphaNumeric(process.env.STAGE).substring(0,5)}-${getAlphaNumeric(projectName).substring(0,12)}-${getAlphaNumeric(model).substring(0,16)}-${dateFormat.asString("yyyyMMddhhmm",new Date())}-${uuidv4().slice(-12)}-f`
}

/**
  Generates a readable and reliabily unique mapping from name to an acceptible format for AWS names
*/
function generateAlphaNumericDash63Name(name) {
  let valid = /^[a-zA-Z0-9](-*[a-zA-Z0-9])*/ 
  if (name.match(valid) && name.length <= 63) {
    return name;
  }
  
  let hashPart = shajs('sha256').update(name).digest('base64').replace(/[\W_]+/g,'').substring(0,24); // remove all non-alphanumeric (+=/) then truncate to 144 bits
  let namePart = getAlphaNumericDash(name).substring(0,63-hashPart.length-2)+'-'; // replace all non-alphanumeric with - and truncate to the proper length
  let result = namePart + hashPart;
  while (result.startsWith('-')) { // can't start with -
    result = result.substring(1);
  }
  return result;
}

/**
 * replace non-alphanumeric with dash (which passes through dashes)
 */
function getAlphaNumericDash(s) {
  return s.replace(/[\W_]+/g,'-')
}

/**
 * remove all non-alphanumeric
 */
function getAlphaNumeric(s) {
  return s.replace(/[^A-Za-z0-9]/g, '')
}


function getProjectNameAndModelFromTransformS3OutputPath(s3OutputPath) {
  if (!s3OutputPath.endsWith('/')) {
    throw new Error(`transform output path ${s3OutputPath} doesn't end with / character`)
  }
  let parts = s3OutputPath.split('/');
  return [parts[parts.length-3], parts[parts.length-2]]
}


function sagemakerBackoff(retryCount) {
  // linear backoff because of 5 minute limit on lambda
  return 1000 + Math.floor(Math.random() * 2000)
}