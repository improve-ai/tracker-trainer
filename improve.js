'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const es = require('event-stream');
const uuidv4 = require('uuid/v4');
const dateFormat = require('date-format');
const _ = require('lodash');

const firehose = new AWS.Firehose();
const s3 = new AWS.S3();
const lambda = new AWS.Lambda();
let sagemaker = new AWS.SageMakerRuntime();

const LOG_PROBABILITY = .1;

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
  let logging = checkShouldLog();
  consoleTime('choose', logging);
  let receivedAt = new Date();

  setup(event, context);
  
  let body = JSON.parse(event.body);
  
  let apiKey = event.requestContext.identity.apiKey;

  if (!apiKey) {
    return error(cb,"'x-api-key' HTTP header required");
  }

  if (!body.model) {
    return error(cb, 'model is required')
  }
  
  if (!body.user_id) {
    return error(cb, 'user_id is required')
  }

  if (!body.variants) {
    return error(cb, 'variants is required')
  }

  for (let propertyKey in body.variants) {
    if (!body.variants.hasOwnProperty(propertyKey)) {
      continue;
    }

    let variants = body.variants[propertyKey];
    if (!Array.isArray(variants)) {
      return error(cb, 'variant values must be lists')
    }
    if (variants.length < 1) {
      return error(cb, "variants must contain at least 1 element")
    }
  }
  
  var params = {
    Body: new Buffer(body),
    EndpointName: getEndpointName(apiKey, body.model)
  };
  
  // TODO if endpoint error, return random variants and log
  
  sagemaker.invokeEndpoint(params).promise().then((response) => {
    consoleTimeEnd('choose', logging)
    // Initiate the callback immediately so that its not blocking on Firehose
    cb(null, {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin" : "*"
      },
      body: response.Body
    });
  }).then((result) => {
    body["record_type"] = "choose";
    // Since we don't initiate firehose until after the response callback,
    // it is possible that this firehose request could be lost if there is
    // an immediate process freeze and the process isn't re-used, but this
    // should happen very infrequently and should not be a problem for most
    // algorithms since they use choose data mostly as hints
    return sendToFirehose(apiKey, body, receivedAt, logging);
  }).catch(error =>{
    consoleTimeEnd('choose', logging)
    console.log(error);
    error(cb,error);
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
    return error(cb,"'x-api-key' HTTP header required");
  }
  
  // if there is no JSON body, just emit the model error
  if (!body || !body.model) {
    return error(cb,"the 'model' field is required");
  }
  
  let valid = /^[a-zA-Z0-9-\._]+$/ 
  if (!body.model.match(valid)) {
    return error(cb, "Only alphanumeric, underscore, period, and dash allowed in model name")
  }

  if (!body.user_id) {
    return error(cb,"the 'user_id' field is required");
  }
  
  if (!body.properties || !(typeof body.properties === 'object')) {
    return error(cb,"the 'properties' object is required");
  }
  
  body["record_type"] = "using";
    
  return sendToFirehose(apiKey, body, receivedAt, logging).then((result) => {
    consoleTimeEnd('using', logging)
    return sendSuccessResponse(cb);
  }).catch(error =>{
    consoleTimeEnd('using', logging)
    console.log(error);
    error(cb,error);
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
    return error(cb,"'x-api-key' HTTP header required");
  }
  
  // if there is no JSON body, just emit the user_id error
  if (!body || !body.user_id) {
    return error(cb,"the 'user_id' field is required");
  }
  
  if (!body.rewards || !(typeof body.rewards === 'object')) {
    return error(cb,"the 'rewards' object is required");
  }
  
  // Check that the rewards are kosher
  for (let rewardKey in body.rewards) {
    if (!body.rewards.hasOwnProperty(rewardKey)) {
      continue;
    }

    let reward = body.rewards[rewardKey];
    if (isNaN(reward) || Number(reward) <= 0) {
      return error(cb,"revenue and reward properties must be positive numbers: "+rewardKey+"="+reward)
    }
  }

  body["record_type"] = "rewards";
  
  return sendToFirehose(apiKey, body, receivedAt, logging).then((result) => {
    consoleTimeEnd('rewards',logging)
    return sendSuccessResponse(cb);
  }).catch(error =>{
    consoleTimeEnd('rewards',logging)
    console.log(error);
    error(cb,error);
  });
}

// Send the event with the timestamp and api key to firehose
function sendToFirehose(apiKey, body, receivedAt, log) {
  body["api_key"] = apiKey;
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

                    if (!requestRecord || !requestRecord.api_key) {
                        console.log(`WARN: no api_key for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    let apiKey = requestRecord.api_key;
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
                    
                    // TODO double check apiKey, model valid chars
                    
                    // apiKey/recordType/(model/)yyyy/MM/dd/hh/apiKey-recordType-(model-)yyyy-MM-dd-hh-mm-ss-uuid.gz
                    let s3Key = getS3Prefix(recordType, apiKey, model)+pathDatePart+"/"+
                      `improve-v3-${apiKey}-${recordType}-`+(model ? `${model}-` : "")+filenameDatePart+"-"+uuidPart+".gz"
                    
                    let buffers = buffersByKey[s3Key]
                    if (!buffers) {
                      buffers = []
                      buffersByKey[s3Key] = buffers
                    }
                    buffers.push(Buffer.from(line+"\n"))

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

module.exports.dispatchParallelTrain = function(event, context, cb) {
  dispatchParallel(process.env.TRAIN_FUNCTION, event, context, cb)
}

function dispatchParallel(functionName, event, context, cb) {

  console.log(`dispatching ${functionName} processes`)

  return listAllApiKeys().then(apiKeys => {
    let promises = []
    
    for (let i = 0; i < apiKeys.length; i++) {
      let apiKey = apiKeys[i]
      let params = {
        Bucket: process.env.RECORDS_BUCKET,
        Delimiter: '/',
        Prefix: `${apiKey}/using/`
      }
      
      console.log(`listing models for apiKey ${apiKey}`)
      // not recursing, so up to 1000 models per apiKey
      promises.push(s3.listObjectsV2(params).promise().then(result => {
        if (!result || !result.CommonPrefixes || !result.CommonPrefixes.length) {
          console.log(`skipping apiKey ${apiKey}`)
          return
        }
        
        return [apiKey, pluckLastPrefixPart(result.CommonPrefixes)]
      }))
    }
  
    return Promise.all(promises)
  }).then(apiKeysAndModels => {
    let promises = []

    for (let i = 0; i < apiKeysAndModels.length; i++) {
      if (!apiKeysAndModels[i]) {
        continue;
      }
      let [apiKey, models] = apiKeysAndModels[i];
      for (let j = 0; j< models.length; j++) {
        let model = models[j]
        
        console.log(`invoking ${functionName} on apiKey ${apiKey} model ${model}`)
        var params = {
          FunctionName: functionName, 
          InvocationType: "Event", 
          Payload: JSON.stringify({api_key: apiKey, model}), 
        }
        
        promises.push(lambda.invoke(params).promise())
      }
    }
    
    return Promise.all(promises)
  }).then(results => {
    return cb(null,'success')
  }, err => {
    return cb(err)
  })
}

module.exports.updateEndpointConfig = function(event, context, cb) {
  var params = {
  CreationTimeAfter: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
  CreationTimeBefore: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
  LastModifiedTimeAfter: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
  LastModifiedTimeBefore: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
  MaxResults: 0,
  NameContains: 'STRING_VALUE',
  NextToken: 'STRING_VALUE',
  SortBy: Name | CreationTime | Status,
  SortOrder: Ascending | Descending,
  StatusEquals: InProgress | Completed | Failed | Stopping | Stopped
};
sagemaker.listTrainingJobs(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});

var params = {
  ExecutionRoleArn: 'STRING_VALUE', /* required */
  ModelName: 'STRING_VALUE', /* required */
  PrimaryContainer: { /* required */
    Image: 'STRING_VALUE', /* required */
    ContainerHostname: 'STRING_VALUE',
    Environment: {
      '<EnvironmentKey>': 'STRING_VALUE',
      /* '<EnvironmentKey>': ... */
    },
    ModelDataUrl: 'STRING_VALUE'
  },
  Tags: [
    {
      Key: 'STRING_VALUE', /* required */
      Value: 'STRING_VALUE' /* required */
    },
    /* more items */
  ],
  VpcConfig: {
    SecurityGroupIds: [ /* required */
      'STRING_VALUE',
      /* more items */
    ],
    Subnets: [ /* required */
      'STRING_VALUE',
      /* more items */
    ]
  }
};
sagemaker.createModel(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});

var params = {
  EndpointConfigName: 'STRING_VALUE', /* required */
  ProductionVariants: [ /* required */
    {
      InitialInstanceCount: process.env.HOSTING_INITIAL_INSTANCE_COUNT,
      InstanceType: process.env.HOSTING_INSTANCE_TYPE,
      ModelName: 'STRING_VALUE',
      VariantName: 'STRING_VALUE'
    },
    /* more items */
  ],
};
sagemaker.createEndpointConfig(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});

var params = {
  EndpointConfigName: 'STRING_VALUE', /* required */
  EndpointName: 'STRING_VALUE', /* required */
};

sagemaker.createEndpoint(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});

}

function createTrainingJob(apiKey, model) {
  
  let recordsS3PrefixBase = "s3://"+process.env.RECORDS_BUCKET+'/'
  let modelsS3PrefixBase = "s3://"+process.env.MODELS_BUCKET+'/'
  
  var params = {
    TrainingJobName: `${apiKey}-${model}`,
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
            S3Uri: recordsS3PrefixBase+getChooseS3KeyPrefix(apiKey, model), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
      {
        ChannelName: 'using',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: recordsS3PrefixBase+getUsingS3KeyPrefix(apiKey, model), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
      {
        ChannelName: 'rewards',
        DataSource: { 
          S3DataSource: { 
            S3DataType:"S3Prefix",
            S3Uri: recordsS3PrefixBase+getRewardsS3KeyPrefix(apiKey), 
            S3DataDistributionType: "FullyReplicated",
          }
        },
      },
    ],
    OutputDataConfig: { 
      S3OutputPath: modelsS3PrefixBase+getModelsS3KeyPrefix(apiKey, model), 
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

function getEndpointName(apiKey, model) {
  return `${process.env.ENDPOINT_BASE_NAME}-${apiKey}-${model}`
}

function getS3KeyPrefix(recordType, apiKey, model) {
  return `${apiKey}/${recordType}/`+(model ? `${model}/` : "")
}

function getRewardsS3KeyPrefix(apiKey) {
  return getS3KeyPrefix("rewards",apiKey)
}

function getUsingS3KeyPrefix(apiKey, model) {
  return getS3KeyPrefix("using",apiKey, model)
}

function getChooseS3KeyPrefix(apiKey, model) {
  return getS3KeyPrefix("choose",apiKey, model)
}

function getModelsS3KeyPrefix(apiKey, model) {
  return `${apiKey}/${model}`
}

function listAllApiKeys(arr, ContinuationToken) {
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
            return listAllApiKeys(arr.concat(pluckLastPrefixPart(result.CommonPrefixes)), result.NextContinuationToken)
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

function error(callback, message) {
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