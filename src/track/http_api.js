'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const LOG_PROBABILITY = .1;

module.exports.track = async function(event, context) {
  let logging = checkShouldLog()
  let receivedAt = new Date()
  
  if (logging) {
    console.log(JSON.stringify(event))
  }

  let body = JSON.parse(event.body)

  if (!body || !body.history_id || !body.message_id) {
    return errorResponse("history_id and message_id fields are required")
  }
  
  body["received_at"] = receivedAt.toISOString()
  // FIX timestamp must never be in the future
  if (!body.timestamp) {
    body["timestamp"] = body["received_at"]
  }
  
  let firehoseRecord = {
    DeliveryStreamName: process.env.FIREHOSE_DELIVERY_STREAM_NAME,
    Record: { 
        Data: Buffer.from(JSON.stringify(body)+'\n')
    }
  }
  
  return firehose.putRecord(firehoseRecord).then(() => {
    return successResponse()
  }).catch(err =>{
    return errorResponse(err)
  })
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