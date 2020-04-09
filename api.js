'use strict';

const AWS = require('aws-sdk');
const uuidv4 = require('uuid/v4');
const _ = require('lodash');
const firehose = new AWS.Firehose();
const sagemakerRuntime = new AWS.SageMakerRuntime();

const unpack_firehose = require("./unpack_firehose.js")
const train_deploy = require("./train_deploy.js")
const customize = require("./customize.js")

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
  
  let projectName = customize.getProjectName(event, context)

  if (!projectName) {
    return sendErrorResponse(cb, "project name not configured");
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
  
  // send to firehose in parallel with sagemaker invoke
  body["record_type"] = "choose";
  sendToFirehose(projectName, body, receivedAt, logging).catch((error) =>
    console.log(error)
  );
  
  var params = {
    Body: new Buffer(event.body),
    EndpointName: train_deploy.getEndpointName(projectName, body.model)
  };
  
  // Invoke the sagemaker endpoint
  sagemakerRuntime.invokeEndpoint(params).promise().then((response) => {
    if (!response.Body) {
      throw new Error("response.Body missing")
    }
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
  })
}

module.exports.track = function(event, context, cb) {
  let logging = checkShouldLog();
  consoleTime('track', logging);
  let receivedAt = new Date();
  
  setup(event, context, logging);
  
  let body = JSON.parse(event.body);
  
  let projectName = customize.getProjectName(event, context)

  if (!projectName) {
    return sendErrorResponse(cb,"project misconfigured or missing credentials")
  }

  if (!body || !body.user_id) {
    return sendErrorResponse(cb,"the 'user_id' field is required")
  }

  return sendToFirehose(projectName, body, receivedAt, logging).then((result) => {
    consoleTimeEnd('track', logging)
    return sendSuccessResponse(cb);
  }).catch(err =>{
    consoleTimeEnd('track', logging)
    console.log(err);
    sendErrorResponse(cb,err);
  });
}

module.exports.using = function(event, context, cb) {
  let logging = checkShouldLog();
  consoleTime('using', logging);
  let receivedAt = new Date();
  
  setup(event, context, logging);
  
  let body = JSON.parse(event.body);
  
  let projectName = customize.getProjectName(event, context)

  if (!projectName) {
    return sendErrorResponse(cb,"project misconfigured or missing credentials");
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
    
  return sendToFirehose(projectName, body, receivedAt, logging).then((result) => {
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
  
  let projectName = customize.getProjectName(event, context)

  if (!projectName) {
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
  
  return sendToFirehose(projectName, body, receivedAt, logging).then((result) => {
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
  // FIX timestamp must never be in the future
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
