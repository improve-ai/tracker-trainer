'use strict';

const AWS = require('aws-sdk');
const dateFormat = require('date-format');
const uuidv4 = require('uuid/v4');
const shajs = require('sha.js')
const _ = require('lodash');

const s3 = new AWS.S3();
const sagemaker = new AWS.SageMaker({ maxRetries: 100, retryDelayOptions: { customBackoff: sagemakerBackoff }});

const customize = require("./customize.js")

const ONE_HOUR_IN_MILLIS = 60 * 60 * 1000;

const recordsS3PrefixBase = "s3://"+process.env.RECORDS_BUCKET+'/'


module.exports.dispatchTrainingJobs = function(event, context, cb) {

  console.log(`dispatching training jobs`)

  let promises = []

  let projectsToModels = customize.getProjectNamesToModelNamesMapping()
  Object.keys(projectsToModels).forEach((projectName) => {
    let models = projectsToModels[projectName]    
    for (let j = 0; j < models.length; j++) {
      let model = models[j]
      promises.push(createFeatureTrainingJob(projectName, model))
    }
  })

  return Promise.all(promises)
}

function createFeatureTrainingJob(projectName, model) {
  
  let hyperparameters = customize.hyperparameters.default;
  
  /* Disabling due to a type mismatch.  hyperparameters expects only strings
  
  if (projectName in config.hyperparameters && model in config.hyperparameters[projectName]) {
    hyperparameters = Object.assign(hyperparameters, config.hyperparameters[projectName][model])
  }*/
  
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
        ChannelName: 'joined',
        CompressionType: 'Gzip',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: recordsS3PrefixBase+getJoinedS3KeyPrefix(projectName, model), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
    ],
    OutputDataConfig: { 
      S3OutputPath: recordsS3PrefixBase+getFeatureModelsS3KeyPrefix(projectName, model), 
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

  return sagemaker.createTrainingJob(params).promise()
}

module.exports.featureModelCreated = function(event, context, cb) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  let promises = []

  if (!event.Records || !event.Records.length > 0) {
    return cb(new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`))
  }

  for (let i = 0; i < event.Records.length; i++) {
    if (!event.Records[i].s3) {
      console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
      continue;
    }

    const s3Record = event.Records[i].s3
    const s3Key = s3Record.object.key

    // feature_models/projectName/modelName/<feature training job>/model.tar.gz
    const [projectName, model, trainingJobName] = s3Record.object.key.split('/').slice(1,4)
    const s3Bucket = s3Record.bucket.name

    // Use the trainingJobName as the ModelName
    let params = {
      ExecutionRoleArn: process.env.FEATURE_TRAINING_ROLE_ARN,
      ModelName: trainingJobName,
      PrimaryContainer: { 
        Image: process.env.FEATURE_TRAINING_IMAGE,
        ModelDataUrl: `s3://${s3Bucket}/${s3Key}`,
      }
    }

    console.log(`Attempting to create model ${trainingJobName}`);
    promises.push(sagemaker.createModel(params).promise().then((response) => {
      if (response.ModelArn) {
        return [projectName, model, trainingJobName]; // trainingJobName is the ModelName
      } else {
        throw new Error("No ModelArn in response, assuming failure");
      }
    }).then(([projectName, model, trainingJobName]) => {
      trainingJobName = "t"+trainingJobName.substring(1)
      return createTransformJob(projectName, model, trainingJobName)
    }))
  }
  
  // Really there should just be just one promise in promises because S3 events are
  // one key at a time, but the format does allow multiple event Records
  return Promise.all(promises).then(results => {
    return cb(null, 'success')
  }, err => {
    return cb(err)
  })
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
          S3Uri: recordsS3PrefixBase+getJoinedS3KeyPrefix(projectName, model), 
        }
      },
      SplitType: "Line",
    },
    TransformOutput: { 
      AssembleWith: "None",
      S3OutputPath: recordsS3PrefixBase+getTransformedS3KeyPrefix(projectName, model), 
    },
    TransformResources: { 
      InstanceType: process.env.TRANSFORM_INSTANCE_TYPE,
      InstanceCount: process.env.TRANSFORM_INSTANCE_COUNT, 
    },
  };
  
  console.log(`Attempting to create transfrorm Job ${trainingJobName}`);
  return sagemaker.createTransformJob(params).promise()
}

module.exports.transformJobCompleted = async function(event, context) {
  console.log(`processing cloudwatch event ${JSON.stringify(event)}`)

  if (!event.detail || !event.detail.TransformJobName || event.detail.TransformJobStatus !== "Completed" ||
      !event.detail.TransformOutput || !event.detail.TransformOutput.S3OutputPath) {
    throw new Error(`WARN: Invalid cloudwatch event ${JSON.stringify(event)}`)
  }
  
  const transformJobName = event.detail.TransformJobName
  const [projectName, model] = getProjectNameAndModelFromS3OutputPath(event.detail.TransformOutput.S3OutputPath)
  
  const trainingJobName = "x"+transformJobName.substring(1)
  
  return createXGBoostTrainingJob(projectName, model, trainingJobName)
}

function createXGBoostTrainingJob(projectName, model, trainingJobName) {
  
  let hyperparameters = customize.hyperparameters.default;
  
  /* Disabling due to a type mismatch.  hyperparameters expects only strings
  
  if (projectName in config.hyperparameters && model in config.hyperparameters[projectName]) {
    hyperparameters = Object.assign(hyperparameters, config.hyperparameters[projectName][model])
  }*/

  console.log(`creating xgboost training job ${trainingJobName} project ${projectName} model ${model}`)
  
  var params = {
    TrainingJobName: trainingJobName,
    HyperParameters: hyperparameters,
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
            S3Uri: recordsS3PrefixBase+getTransformedS3KeyPrefix(projectName, model), 
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
            S3Uri: recordsS3PrefixBase+getTransformedS3KeyPrefix(projectName, model), 
            S3DataDistributionType: "ShardedByS3Key",
          }
        },
      },
    ],
    OutputDataConfig: { 
      S3OutputPath: recordsS3PrefixBase+getXGBoostModelsS3KeyPrefix(projectName, model), 
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

module.exports.deployUpdatedModels = function(event, context, cb) {

  return listRecentlyCompletedTrainingJobs().then((trainingJobs) => {
    let promises = []
    for (let i=0;i<trainingJobs.length;i++) {
      // delay to avoid throttling errors
      promises.push(delay(500*i).then(() => {
        maybeCreateOrUpdateEndpointForTrainingJob(trainingJobs[i].TrainingJobName)
      }));
    }
    
    return Promise.all(promises)
  }).then((results) => {
    console.log(results);
  })

}

function maybeCreateOrUpdateEndpointForTrainingJob(trainingJobName) {
  let params = {
    TrainingJobName: trainingJobName 
  }
  
  return sagemaker.describeTrainingJob(params).promise().then((trainingJobDescription) => {
    if (!trainingJobDescription || !trainingJobDescription.ModelArtifacts || !trainingJobDescription.ModelArtifacts.S3ModelArtifacts) {
      throw new Error("Model artifacts not found in TrainingJobDescription");
    }
    if (!trainingJobDescription.OutputDataConfig || !trainingJobDescription.OutputDataConfig.S3OutputPath) {
      throw new Error("S3OutputPath not found in TrainingJobDescription");
    }
    if (trainingJobDescription.TrainingJobStatus !== "Completed") {
      throw new Error(`TrainingJobStatus ${trainingJobDescription.TrainingJobStatus} is not 'Completed'`)
    }
    
    // Use the trainingJobName as the ModelName
    let params = {
      ExecutionRoleArn: process.env.FEATURE_TRAINING_ROLE_ARN,
      ModelName: trainingJobName,
      PrimaryContainer: { 
        Image: process.env.HOSTING_IMAGE,
        ModelDataUrl: trainingJobDescription.ModelArtifacts.S3ModelArtifacts,
      }
    }
    
    let [projectName, model] = getProjectNameAndModelFromS3OutputPath(trainingJobDescription.OutputDataConfig.S3OutputPath)
    console.log(`Attempting to create model ${trainingJobName}`);
    return sagemaker.createModel(params).promise().then((response) => {
      if (response.ModelArn) {
        return [projectName, model, trainingJobName]; // trainingJobName is the ModelName
      } else {
        throw new Error("No ModelArn in response, assuming failure");
      }
    });
  }).then(([projectName, model, trainingJobName]) => {

    // Use the trainingJobName/ModelName as the EndpointConfigName
    let params = {
      EndpointConfigName: trainingJobName,
      ProductionVariants: [ 
        {
          InitialInstanceCount: customize.hyperparameters[projectName][model].hosting_initial_instance_count || customize.hyperparameters.default.hosting_initial_instance_count || process.env.HOSTING_INITIAL_INSTANCE_COUNT,
          InstanceType: process.env.HOSTING_INSTANCE_TYPE,
          ModelName: trainingJobName,
          VariantName: "A",
        },
      ],
    };

    console.log(`Creating EndpointConfig ${trainingJobName}`);
    return sagemaker.createEndpointConfig(params).promise().then((response) => {
      if (response.EndpointConfigArn) {
        return [projectName, model, trainingJobName]; // trainingJobName is the EndpointConfigName
      } else {
        throw new Error(`No EndpointConfigArn in response ${JSON.stringify(response)}, assuming failure`);
      }
    });
  }).then(([projectName, model, EndpointConfigName]) => {
    console.log(`projectName ${projectName} model ${model} EndpointConfigName ${EndpointConfigName}`)
    let EndpointName = getEndpointName(projectName, model)
    let params = {
      EndpointName
    }
    
    sagemaker.describeEndpoint(params).promise().then((result) => {
      console.log(result)
    }).catch((error) => {
      let params = {
        EndpointConfigName,
        EndpointName,
      };
      console.log(error)

      console.log(`Creating Endpoint ${EndpointName} EndpointConfigName ${EndpointConfigName}`)
      return sagemaker.createEndpoint(params).promise().then((result) => {
        console.log(result);
      });

    }).then((result) => {
      let params = {
        EndpointConfigName,
        EndpointName,
      };
  
      console.log(`Updating Endpoint ${EndpointName} EndpointConfigName ${EndpointConfigName}`)
      return sagemaker.updateEndpoint(params).promise().then((result) => {
        console.log(result);
      });
      
    });
  }).catch((error) => {
    // Handle errors within the function since this is a conditional update
    console.log(error);
  });
}

function listRecentlyCompletedTrainingJobs(arr, params) {
  console.log(`listing training jobs${params ? " with params "+params: ""}`)

  if (!arr) arr=[];

  if (!params) {
    params = {
      LastModifiedTimeAfter: new Date(new Date().getTime() - ONE_HOUR_IN_MILLIS),
      // NameContains: 'STRING_VALUE', FIX Scope to only improve.ai training jobs
      StatusEquals: "Completed",
      MaxResults: 100,
    };
  }
  return sagemaker.listTrainingJobs(params).promise().then(result => {
      if (!result || !result.TrainingJobSummaries || !result.TrainingJobSummaries.length) {
          return arr;
      } 
      
      arr = arr.concat(result.TrainingJobSummaries);
      
      if (result.NextToken) {
        params["NextToken"] = result.NextToken
        return listRecentlyCompletedTrainingJobs(arr, params)
      } else {
        return arr;
      }
  })
}

function getEndpointName(projectName, model) {
  // this is a somewhat readable format for the sagemaker console
  return generateAlphaNumericDash63Name(`${process.env.STAGE}-${model}-${projectName}-${process.env.SERVICE}`);
}

module.exports.getEndpointName = getEndpointName;

function getFeatureTrainingJobName(projectName, model) {
  
  // every single training job must have a unique name per AWS account
  return `f-${getAlphaNumeric(process.env.STAGE).substring(0,5)}-${getAlphaNumeric(projectName).substring(0,10)}-${getAlphaNumeric(model).substring(0,16)}-${dateFormat.asString("yyyyMMddhhmmss",new Date())}-${uuidv4().slice(-12)}`
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

function getJoinedS3KeyPrefix(projectName, model) {
  return `joined/${projectName}/${model}`
}

function getFeatureModelsS3KeyPrefix(projectName, model) {
  return `feature_models/${projectName}/${model}`
}

function getXGBoostModelsS3KeyPrefix(projectName, model) {
  return `xgboost_models/${projectName}/${model}`
}

function getTransformedS3KeyPrefix(projectName, model) {
  return `transformed/${projectName}/${model}`
}

function getProjectNameAndModelFromS3OutputPath(S3OutputPath) {
  let parts = S3OutputPath.split('/');
  return [parts[parts.length-2], parts[parts.length-1]]
}

function pluckLastPrefixPart(arr) {
  return _.map(_.map(arr, "Prefix"), item => {
    return item.split('/').slice(-2)[0] // split and grab to second to last item
  })
}

// from https://stackoverflow.com/questions/39538473/using-settimeout-on-promise-chain
function delay(t, v) {
   return new Promise(function(resolve) { 
       setTimeout(resolve.bind(null, v), t)
   });
}

function sagemakerBackoff(retryCount) {
  // linear backoff because of 5 minute limit on lambda
  return 1000 + Math.floor(Math.random() * 2000)
}