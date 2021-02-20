'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();
const uuidv4 = require('uuid/v4');

const customize = require("./customize.js")
const naming = require("./naming.js")

const LOG_PROBABILITY = .1;

module.exports.track = async function(event, context) {
  let logging = checkShouldLog()
  consoleTime('track', logging)
  let receivedAt = new Date()
  
  if (logging) {
    console.log(JSON.stringify(event))
  }

  let body = JSON.parse(event.body)

  if (!body || !body.history_id) {
    return errorResponse("the 'history_id' field is required")
  }
  
  let projectName = customize.projectNameForTrack(event, context)

  if (!projectName || !naming.isValidProjectName(projectName)) {
    console.log(`WARN: invalid project name ${projectName}, not alphanumeric, underscore, dash, space, period ${JSON.stringify(event)}`)
    return errorResponse("project misconfigured or missing credentials")
  }

  return sendToFirehose(projectName, body, receivedAt, logging).then(() => {
    consoleTimeEnd('track', logging)
    return successResponse()
  }).catch(err =>{
    consoleTimeEnd('track', logging)
    return errorResponse(err)
  })
}

// Send the event with the timestamp and project name to firehose
function sendToFirehose(projectName, body, receivedAt, log) {
  body["project_name"] = projectName;
  body["received_at"] = receivedAt.toISOString();
  // FIX timestamp must never be in the future
  if (!body.timestamp) {
    body["timestamp"] = body["received_at"];
  }
  if (!body.message_id) {
    body["message_id"] = uuidv4()
  }
  let firehoseData = Buffer.from(JSON.stringify(body)+'\n')
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


function successResponse() {
  return {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin" : "*"
    },
    body: JSON.stringify({ status: "success" })
  }
}

function errorResponse(message) {
  console.log(message)
  const response = {
    statusCode: 400,
    body: JSON.stringify({ error: { message: message}})
  }
  return response
}

function checkShouldLog() {
  return Math.random() < LOG_PROBABILITY;
}

function consoleTime(name, shouldLog) {
  if (shouldLog) {
    console.time(name)
  }
}

function consoleTimeEnd(name, shouldLog) {
  if (shouldLog) {
    console.timeEnd(name)
  }
}
