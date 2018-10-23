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

const LOG_PROBABILITY = .1;

function setup(event, context, shouldLog) {
  context.callbackWaitsForEmptyEventLoop = false; // allow persistent redis connection
  
  if (shouldLog) {
    //console.log(JSON.stringify(context));
    console.log(JSON.stringify(event));
    console.log(event.body);
  }
}

function error(callback, message) {
  let response = JSON.stringify({ "error": { "message": message}});
  console.log(response);
  return callback(null, {
    statusCode: 400,
    body: response
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
                    
                    let model = requestRecord.model;
                    
                    if ((recordType === "choose" || recordType === "using") && !model) {
                        console.log(`WARN: no model for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    // TODO double check apiKey, model valid chars
                    
                    // apiKey/recordType/(model/)yyyy/MM/dd/hh/apiKey-recordType-(model-)yyyy-MM-dd-hh-mm-ss-uuid.gz
                    let s3Key = `${apiKey}/${recordType}/`+(model ? `${model}/` : "")+pathDatePart+"/"+
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

module.exports.dispatchParallelJoin = function(event, context, cb) {
  dispatchParallel(process.env.JOIN_FUNCTION, event, context, cb)
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