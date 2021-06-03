'use strict';

const AWS = require('aws-sdk');
const dateFormat = require('date-format');
const uuidv4 = require('uuid/v4');
const shajs = require('sha.js')
const _ = require('lodash');

const s3 = new AWS.S3();
const sagemaker = new AWS.SageMaker();

const naming = require("./naming.js")

module.exports.dispatchTrainingJobs = async () => {

  console.log(`dispatching training jobs`)
  return Promise.all(Object.entries(naming.getModelsByProject()).map(([projectName, models]) => 
    Promise.all(models.map(model => 
      createTrainingJob(projectName, model)))))
}

function createTrainingJob(projectName, model, trainingJobName) {
  
  
  console.log(`creating xgboost training job ${trainingJobName} project ${projectName} model ${model} params ${JSON.stringify(params)}`)
  const hyperparameters = naming.getXGBoostHyperparameters(projectName, model)
  
  var params = {
    TrainingJobName: trainingJobName,
    HyperParameters: hyperparameters,
    AlgorithmSpecification: { /* required */
      TrainingImage: process.env.XGBOOST_TRAINING_IMAGE,
      TrainingInputMode: "Pipe",
    },
    InputDataConfig: [
      {
        ChannelName: 'train',
        CompressionType: 'None',
        ContentType: 'text/csv',
        DataSource: {
          S3DataSource: {
            S3DataType:"S3Prefix",
            S3Uri: naming.getTransformedTrainS3Uri(projectName, model),
            S3DataDistributionType: "ShardedByS3Key",
          }
        },
      },
     /* {
        ChannelName: 'validation',
        CompressionType: 'None',
        ContentType: 'text/csv',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: naming.getTransformedValidationS3Uri(projectName, model), 
            S3DataDistributionType: "ShardedByS3Key",
          }
        },
      },*/
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