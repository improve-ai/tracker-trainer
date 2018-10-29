'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const es = require('event-stream');
const uuidv4 = require('uuid/v4');
const dateFormat = require('date-format');
const _ = require('lodash');
var shajs = require('sha.js')

const firehose = new AWS.Firehose();
const s3 = new AWS.S3();
const sagemakerRuntime = new AWS.SageMakerRuntime();
const sagemaker = new AWS.SageMaker({ maxRetries: 100, retryDelayOptions: { customBackoff: sagemakerBackoff }});

const LOG_PROBABILITY = .1;
const ONE_HOUR_IN_MILLIS = 60 * 60 * 1000;


function setup(event, context, shouldLog) {
  /* Set callbackWaitsForEmptyEventLoop=false to allow choose() to return a response
     immediately while the firehose payload is sent in the background.  According
     to AWS documentation, it is possible that the choose firehose could be 
     lost, but for choose() this is typically not a big deal since the
     the algorithms use it for sampling typical properties instead of direct
     training.  Firehose requests for 'using' and 'rewards' are verified prior 
     to sending a response.
  
     From the AWS docs:
     
     callbackWaitsForEmptyEventLoop: The default value is true. This property 
     is useful only to modify the default behavior of the callback.  By default,
     the callback will wait until the event loop is empty before freezing the
     process and returning the results to the caller. You can set this property
     to false to request AWS Lambda to freeze the process soon after the
     callback is called, even if there are events in the event loop. AWS Lambda
     will freeze the process, any state data and the events in the event loop
     (any remaining events in the event loop processed when the Lambda function
     is called next and if AWS Lambda chooses to use the frozen process).
  */
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (shouldLog) {
    //console.log(JSON.stringify(context));
    console.log(JSON.stringify(event));
    console.log(event.body);
  }
}

module.exports.choose = function(event, context, cb) {
  //let logging = checkShouldLog();
  let logging = true;
  consoleTime('choose', logging);
  let receivedAt = new Date();

  setup(event, context);
  
  let body = JSON.parse(event.body);
  
  let apiKey = event.requestContext.identity.apiKey;

  if (!apiKey) {
    return sendErrorResponse(cb,"'x-api-key' HTTP header required");
  }

  if (!body.model) {
    return sendErrorResponse(cb, 'model is required')
  }
  
  if (!body.user_id) {
    return sendErrorResponse(cb, 'user_id is required')
  }

  if (!body.variants || !(typeof body.variants === 'object')) {
    return sendErrorResponse(cb, "the 'variants' object is required")
  }

  for (let propertyKey in body.variants) {
    if (!body.variants.hasOwnProperty(propertyKey)) {
      continue;
    }

    let variants = body.variants[propertyKey];
    if (!Array.isArray(variants)) {
      return sendErrorResponse(cb, 'variant values must be lists')
    }
    if (variants.length < 1) {
      return sendErrorResponse(cb, "variants must contain at least 1 element")
    }
  }
  
  var params = {
    Body: new Buffer(event.body),
    EndpointName: getEndpointName(apiKey, body.model)
  };
  
  sagemakerRuntime.invokeEndpoint(params).promise().then((response) => {
    if (!response.Body) {
      throw new Error("response.Body missing")
    }
    console.log(response.Body.toString('utf8'));
    consoleTimeEnd('choose', logging)
    // Initiate the callback immediately so that its not blocking on Firehose
    cb(null, {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin" : "*"
      },
      body: response.Body.toString('utf8')
    });
  }).catch((err) => {
    consoleTimeEnd('choose', logging)
    console.log(err);
    console.log("Error invoking sagemaker endpoint - returning random variants")
    let response = {
      properties: chooseRandomVariants(body.variants)
    }
    cb(null, {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin" : "*"
      },
      body: JSON.stringify(response)
    });
  }).then((result) => {
    body["record_type"] = "choose";
    // Since we don't initiate firehose until after the response callback,
    // it is possible that this firehose request could be lost if there is
    // an immediate process freeze and the process isn't re-thawed, but this
    // should happen very infrequently and should not be a problem for most
    // algorithms since they use choose data mostly as hints
    return sendToFirehose(apiKey, body, receivedAt, logging);
  });
}

module.exports.using = function(event, context, cb) {
  let logging = checkShouldLog();
  consoleTime('using', logging);
  let receivedAt = new Date();
  
  setup(event, context, logging);
  
  let body = JSON.parse(event.body);
  
  let apiKey = event.requestContext.identity.apiKey;

  if (!apiKey) {
    return sendErrorResponse(cb,"'x-api-key' HTTP header required");
  }
  
  // if there is no JSON body, just emit the model error
  if (!body || !body.model) {
    return sendErrorResponse(cb,"the 'model' field is required");
  }
  
  let valid = /^[a-zA-Z0-9-\._]+$/ 
  if (!body.model.match(valid)) {
    return sendErrorResponse(cb, "Only alphanumeric, underscore, period, and dash allowed in model name")
  }

  if (!body.user_id) {
    return sendErrorResponse(cb,"the 'user_id' field is required");
  }
  
  if (!body.properties || !(typeof body.properties === 'object')) {
    return sendErrorResponse(cb,"the 'properties' object is required");
  }
  
  body["record_type"] = "using";
    
  return sendToFirehose(apiKey, body, receivedAt, logging).then((result) => {
    consoleTimeEnd('using', logging)
    return sendSuccessResponse(cb);
  }).catch(err =>{
    consoleTimeEnd('using', logging)
    console.log(err);
    sendErrorResponse(cb,err);
  });
}

module.exports.rewards = function(event, context, cb) {
  let logging = checkShouldLog();
  consoleTime('rewards',logging)

  let receivedAt = new Date();
  
  setup(event, context, logging);
  
  let body = JSON.parse(event.body);
  
  let apiKey = event.requestContext.identity.apiKey;

  if (!apiKey) {
    return sendErrorResponse(cb,"'x-api-key' HTTP header required");
  }
  
  // if there is no JSON body, just emit the user_id error
  if (!body || !body.user_id) {
    return sendErrorResponse(cb,"the 'user_id' field is required");
  }
  
  if (!body.rewards || !(typeof body.rewards === 'object')) {
    return sendErrorResponse(cb,"the 'rewards' object is required");
  }
  
  // Check that the rewards are kosher
  for (let rewardKey in body.rewards) {
    if (!body.rewards.hasOwnProperty(rewardKey)) {
      continue;
    }

    let reward = body.rewards[rewardKey];
    if (isNaN(reward) || Number(reward) <= 0) {
      return sendErrorResponse(cb,"revenue and reward properties must be positive numbers: "+rewardKey+"="+reward)
    }
  }

  body["record_type"] = "rewards";
  
  return sendToFirehose(apiKey, body, receivedAt, logging).then((result) => {
    consoleTimeEnd('rewards',logging)
    return sendSuccessResponse(cb);
  }).catch(err =>{
    consoleTimeEnd('rewards',logging)
    console.log(err);
    sendErrorResponse(cb,err);
  });
}

// Send the event with the timestamp and project name to firehose
function sendToFirehose(projectName, body, receivedAt, log) {
  body["project_name"] = projectName;
  body["received_at"] = receivedAt.toISOString();
  if (!body["timestamp"]) {
    body["timestamp"] = body["received_at"];
  }
  if (!body["message_id"]) {
    body["message_id"] = uuidv4()
  }
  let firehoseData = new Buffer(JSON.stringify(body)+'\n')
  consoleTime('firehose',log)
  consoleTime('firehose-create',log)

  let firehosePromise = firehose.putRecord({
    DeliveryStreamName: process.env.FIREHOSE_DELIVERY_STREAM_NAME,
    Record: { 
        Data: firehoseData
    }
  }).promise().then(result => {
    consoleTimeEnd('firehose',log)
    return result
  })
  consoleTimeEnd('firehose-create',log)
  return firehosePromise;
}

function chooseRandomVariants(variantMap) {
  let properties = {}
  for (let propertyKey in variantMap) {
    if (!variantMap.hasOwnProperty(propertyKey)) {
      continue;
    }

    properties[propertyKey] = _.sample(variantMap[propertyKey]);
  }
  return properties;
}

module.exports.unpackFirehose = function(event, context, cb) {

  console.log(`processing event records from firehose bucket SNS event ${JSON.stringify(event)}`)
  
  let now = new Date()
  let pathDatePart = dateFormat.asString("yyyy/MM/dd/hh", now)
  let filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss",now)
  let uuidPart = uuidv4() // use the same uuid across the files for this unpacking
  
  let buffersByKey = {}
  let promises = []
  
  for (let i = 0 ; i < event.Records.length ; i++) {
    if (!event.Records[i].Sns || !event.Records[i].Sns.Message) {
        console.log(`WARN: Invalid S3 SNS notification ${JSON.stringify(event)}`)
        continue;
    }
    let snsMessage = JSON.parse(event.Records[i].Sns.Message)
    for (let j = 0 ; j < snsMessage.Records.length ; j++) {

        promises.push(new Promise((resolve, reject) => {
            
            let s3Record = snsMessage.Records[j].s3
    
            let gunzip = zlib.createGunzip()
            
            let stream = s3.getObject({
                Bucket: s3Record.bucket.name,
                Key: s3Record.object.key,
            }).createReadStream()
            .pipe(gunzip)
            .pipe(es.split()) // parse line-by-line
            .pipe(es.mapSync(function(line){
        
                // pause the readstream
                stream.pause();

                try {
                    if (!line) {
                        return;
                    }
                    let requestRecord = JSON.parse(line)

                    if (!requestRecord || !requestRecord.project_name) {
                        console.log(`WARN: no project_name for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    let projectName = requestRecord.project_name;
                    
                    // delete project_name from requestRecord in case its sensitive
                    delete requestRecord.project_name;
                    
                    let recordType = requestRecord.record_type;
                    
                    if (!recordType) {
                        console.log(`WARN: no record_type for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    if (!(recordType === "choose" || recordType === "using" || recordType === "rewards")) {
                        console.log(`WARN: invalid record_type for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    let model = null; // leave as null in case its a rewards record
                    
                    if (recordType === "choose" || recordType === "using") {
                      model = requestRecord.model;
                      if (!model) {
                        console.log(`WARN: no model for requestRecord ${requestRecord}`)
                        return;
                      }
                    }
                    
                    // TODO double check projectName, model valid chars
                    
                    // projectName/recordType/(model/)yyyy/MM/dd/hh/projectName-recordType-(model-)yyyy-MM-dd-hh-mm-ss-uuid.gz
                    let s3Key = getS3KeyPrefix(recordType, projectName, model)+pathDatePart+"/"+
                      `improve-v3-${projectName}-${recordType}-`+(model ? `${model}-` : "")+filenameDatePart+"-"+uuidPart+".gz"
                    
                    let buffers = buffersByKey[s3Key]
                    if (!buffers) {
                      buffers = []
                      buffersByKey[s3Key] = buffers
                    }
                    buffers.push(Buffer.from(JSON.stringify(requestRecord)+"\n"))

                } catch (err) {
                    console.log(`error ${err} skipping requestRecord`)
                } finally {
                    stream.resume();
                }
            })
            .on('error', function(err){
                console.log('Error while reading file.', err);
                return reject(err)
            })
            .on('end', function(){
                return resolve()
            }));
        }))
    }
  }

  return Promise.all(promises).then(results => {
    // ignore results, just use the shared buffersByKey
    
    let promises = []

    for (var s3Key in buffersByKey) {
      if (!buffersByKey.hasOwnProperty(s3Key)) {
        continue
      }
      
      let buffers = buffersByKey[s3Key]
      if (!buffers.length) {
        continue;
      }

      console.log(`writing ${buffers.length} records to ${s3Key}`)

      let params = {
        Body: zlib.gzipSync(Buffer.concat(buffers)), 
        Bucket: process.env.RECORDS_BUCKET, 
        Key: s3Key
      }
      
      promises.push(s3.putObject(params).promise())
    }

    return Promise.all(promises)
  }).then(results => {
    return cb(null,'success')
  }, err => {
    return cb(err)
  })
}

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
      /* '<ParameterKey>': ... */
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

  // FIX problem with listTrainingJobs doesn't allow us to use LastModifiedAfterr
  return listSomeTrainingJobs(20).then((trainingJobs) => {
    let promises = []
    for (let i=0;i<trainingJobs.length;i++) {
      // delay to avoid throttling errors
      promises.push(delay(500*i).then(() => {
        maybeCreateOrUpdateEndpointForTrainingJob(trainingJobs[i].TrainingJobName)
      }));
    }
    
    return Promise.all(promises)
  }).then((trainingJobDescriptions) => {
    console.log(JSON.stringify(trainingJobDescriptions));
    //return createModel()
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
    console.log(`Got projectName ${projectName} model ${model} from S3OutputPath`)

    console.log(`Creating Model ${trainingJobName}`);
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
      sagemaker.updateEndpoint(params).promise().then((result) => {
        console.log(result);
      });
      
    });
  }).catch((error) => {
    // Handle errors within the function since this is a conditional update
    console.log(error);
  });
}


function getEndpointName(projectName, model) {
  // this is a somewhat readable format for the sagemaker console
  return generateAlphaNumericDash63Name(`${process.env.STAGE}-${model}-${projectName}-${process.env.SERVICE}`);
}

function getTrainingJobName(projectName, model) {
  // every single training job must have a unique name per AWS account
  return generateAlphaNumericDash63Name(`${process.env.STAGE}-${model}-${projectName}-${process.env.SERVICE}-${uuidv4()}`)
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
  let namePart = name.replace(/[\W_]+/g,'-').substring(0,63-hashPart.length-2)+'-'; // replace all non-alphanumeric with - and truncate to the proper length
  let result = namePart + hashPart;
  while (result.startsWith('-')) { // can't start with -
    result = result.substring(1);
  }
  return result;
}

function getS3KeyPrefix(recordType, projectName, model) {
  return `${projectName}/${recordType}/`+(model ? `${model}/` : "")
}

function getRewardsS3KeyPrefix(projectName) {
  return getS3KeyPrefix("rewards",projectName)
}

function getUsingS3KeyPrefix(projectName, model) {
  return getS3KeyPrefix("using",projectName, model)
}

function getChooseS3KeyPrefix(projectName, model) {
  return getS3KeyPrefix("choose",projectName, model)
}

function getModelsS3KeyPrefix(projectName, model) {
  return `${projectName}/${model}`
}

function getProjectNameAndModelFromS3OutputPath(S3OutputPath) {
  let parts = S3OutputPath.split('/');
  return [parts[parts.length-2], parts[parts.length-1]]
}

function listSomeTrainingJobs(MaxResults) {
  let params = {
    LastModifiedTimeAfter: new Date(new Date().getTime() - ONE_HOUR_IN_MILLIS),
    // NameContains: 'STRING_VALUE', FIX Scope to only improve.ai training jobs
    StatusEquals: "Completed",
    MaxResults,
  };

  return sagemaker.listTrainingJobs(params).promise().then(result => {
    console.log(JSON.stringify(result))
    if (!result || !result.TrainingJobSummaries || !result.TrainingJobSummaries.length) {
      return [];
    } 
    
    return result.TrainingJobSummaries;
  })
}


function listAllTrainingJobs(arr, NextToken) {
  console.log(`listing training jobs${NextToken ? " at position "+NextToken: ""}`)

  if (!arr) arr=[];

  let params;

  if (NextToken) {
    params = {
      NextToken
    };
  } else {
    params = {
      LastModifiedTimeAfter: new Date(new Date().getTime() - ONE_HOUR_IN_MILLIS),
      // NameContains: 'STRING_VALUE', FIX Scope to only improve.ai training jobs
      StatusEquals: "Completed",
      MaxResults: 100,
    };
  }

  return sagemaker.listTrainingJobs(params).promise().then(result => {
      console.log(result)
      if (!result || !result.TrainingJobSummaries || !result.TrainingJobSummaries.length) {
          return arr;
      } 
      
      arr = arr.concat(result.TrainingJobSummaries);
      
      if (result.NextToken) {
        return listAllTrainingJobs(arr, result.NextToken)
      } else {
        return arr;
      }
  })
}

function listAllProjects(arr, ContinuationToken) {
  console.log(`listing all API keys${ContinuationToken ? " at position "+ContinuationToken: ""}`)

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

function pluckLastPrefixPart(arr) {
  return _.map(_.map(arr, "Prefix"), item => {
    return item.split('/').slice(-2)[0] // split and grab to second to last item
  })
}

function sendSuccessResponse(callback) {
  let response = {
    status: "success"
  };
  
  return callback(null, {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin" : "*"
    },
    body: JSON.stringify(response)
  });
}

function sendErrorResponse(callback, message) {
  let response = JSON.stringify({ "error": { "message": message}});
  console.log(response);
  return callback(null, {
    statusCode: 400,
    body: response
  });
}

function checkShouldLog() {
  return Math.random() < LOG_PROBABILITY;
}

function consoleTime(name, shouldLog) {
  if (shouldLog) {
    console.time(name);
  }
}

function consoleTimeEnd(name, shouldLog) {
  if (shouldLog) {
    console.timeEnd(name);
  }
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