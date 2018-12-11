'use strict';

const AWS = require('aws-sdk');
const dateFormat = require('date-format');
const uuidv4 = require('uuid/v4');
const shajs = require('sha.js')
const _ = require('lodash');

const s3 = new AWS.S3();
const sagemaker = new AWS.SageMaker({ maxRetries: 100, retryDelayOptions: { customBackoff: sagemakerBackoff }});

const unpack_firehose = require("./unpack_firehose.js")

const ONE_HOUR_IN_MILLIS = 60 * 60 * 1000;

module.exports.dispatchTrainingJobs = function(event, context, cb) {

  console.log(`dispatching training jobs`)

  return listAllProjects().then(projectNames => {
    let promises = []
    
    for (let i = 0; i < projectNames.length; i++) {
      let projectName = projectNames[i]
      let params = {
        Bucket: process.env.RECORDS_BUCKET,
        Delimiter: '/',
        Prefix: `${projectName}/using/`
      }
      
      console.log(`listing models for projectName ${projectName}`)
      // not recursing, so up to 1000 models per projectName
      promises.push(s3.listObjectsV2(params).promise().then(result => {
        if (!result || !result.CommonPrefixes || !result.CommonPrefixes.length) {
          console.log(`skipping projectName ${projectName}`)
          return
        }
        
        return [projectName, pluckLastPrefixPart(result.CommonPrefixes)]
      }))
    }
  
    return Promise.all(promises)
  }).then(projectNamesAndModels => {
    let promises = []

    for (let i = 0; i < projectNamesAndModels.length; i++) {
      if (!projectNamesAndModels[i]) {
        continue;
      }
      let [projectName, models] = projectNamesAndModels[i];
      for (let j = 0; j< models.length; j++) {
        let model = models[j]
        console.log(`creating training job for project ${projectName} model ${model}`)
        promises.push(createTrainingJob(projectName, model))
      }
    }
    
    return Promise.all(promises)
  }).then(results => {
    return cb(null,'success')
  }, err => {
    return cb(err)
  })
}


function createTrainingJob(projectName, model) {
  
  let recordsS3PrefixBase = "s3://"+process.env.RECORDS_BUCKET+'/'
  let modelsS3PrefixBase = "s3://"+process.env.MODELS_BUCKET+'/'
  
  var params = {
    TrainingJobName: getTrainingJobName(projectName, model),
    HyperParameters: {
      objective: "binary:logistic",
      max_age: 60 * 60 * 24 * 90
    },
    AlgorithmSpecification: { /* required */
      TrainingImage: process.env.TRAINING_IMAGE,
      TrainingInputMode: "File",
    },
    InputDataConfig: [ 
      {
        ChannelName: 'choose',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: recordsS3PrefixBase+getChooseS3KeyPrefix(projectName, model), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
      {
        ChannelName: 'using',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: recordsS3PrefixBase+getUsingS3KeyPrefix(projectName, model), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
      {
        ChannelName: 'rewards',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: recordsS3PrefixBase+getRewardsS3KeyPrefix(projectName), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
    ],
    OutputDataConfig: { 
      S3OutputPath: modelsS3PrefixBase+getModelsS3KeyPrefix(projectName, model), 
    },
    ResourceConfig: { 
      InstanceCount: 1, 
      InstanceType: process.env.TRAINING_INSTANCE_TYPE,
      VolumeSizeInGB: process.env.TRAINING_VOLUME_SIZE_IN_GB
    },
    RoleArn: process.env.TRAINING_ROLE_ARN,
    StoppingCondition: { 
      MaxRuntimeInSeconds: process.env.TRAINING_MAX_RUNTIME_IN_SECONDS,
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
      ExecutionRoleArn: process.env.TRAINING_ROLE_ARN,
      ModelName: trainingJobName,
      PrimaryContainer: { 
        Image: process.env.HOSTING_IMAGE,
        ModelDataUrl: trainingJobDescription.ModelArtifacts.S3ModelArtifacts,
      }
    }
    
    let [projectName, model] = getProjectNameAndModelFromS3OutputPath(trainingJobDescription.OutputDataConfig.S3OutputPath)
    console.log(`Attempting to Create Model ${trainingJobName}`);
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
          InitialInstanceCount: process.env.HOSTING_INITIAL_INSTANCE_COUNT,
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

function listAllProjects(arr, ContinuationToken) {
  console.log(`listing all projects${ContinuationToken ? " at position "+ContinuationToken: ""}`)

  if (!arr) arr=[];

  const params = {
      Bucket: process.env.RECORDS_BUCKET,
      Delimiter: '/',
      ContinuationToken
  }
  
  return s3.listObjectsV2(params).promise().then(result => {
      if (!result || !result.CommonPrefixes || !result.CommonPrefixes.length) {
          return arr;
      } else if (!result.IsTruncated) {
          return arr.concat(pluckLastPrefixPart(result.CommonPrefixes))
      } else {
          return listAllProjects(arr.concat(pluckLastPrefixPart(result.CommonPrefixes)), result.NextContinuationToken)
      }
  })
}


function getEndpointName(projectName, model) {
  // this is a somewhat readable format for the sagemaker console
  return generateAlphaNumericDash63Name(`${process.env.STAGE}-${model}-${projectName}-${process.env.SERVICE}`);
}

module.exports.getEndpointName = getEndpointName;

function getTrainingJobName(projectName, model) {
  
  // every single training job must have a unique name per AWS account
  return `${getAlphaNumeric(process.env.STAGE).substring(0,5)}-${getAlphaNumeric(projectName).substring(0,12)}-${getAlphaNumeric(model).substring(0,16)}-${dateFormat.asString("yyyyMMddhhmmss",new Date())}-${uuidv4().slice(-12)}`
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

function getRewardsS3KeyPrefix(projectName) {
  return unpack_firehose.getS3KeyPrefix("rewards",projectName)
}

function getUsingS3KeyPrefix(projectName, model) {
  return unpack_firehose.getS3KeyPrefix("using",projectName, model)
}

function getChooseS3KeyPrefix(projectName, model) {
  return unpack_firehose.getS3KeyPrefix("choose",projectName, model)
}

function getModelsS3KeyPrefix(projectName, model) {
  return `${projectName}/${model}`
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