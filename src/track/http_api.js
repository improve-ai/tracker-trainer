'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const LOG_PROBABILITY = .01;

module.exports.track = async function(event, context) {
  const receivedAt = new Date()
  
  if (checkShouldLog()) {
    console.log(JSON.stringify(event))
  }

  const record = JSON.parse(event.body)

  const historyId = record.history_id
  
  if (!historyId || typeof historyId !== "string") {
    return errorResponse("history_id field is required")
  }
  
  const messageId = record.message_id
  
  if (!messageId || typeof messageId !== "string") {
    return errorResponse("message_id field is required")
  }
  
  const timestamp = record.timestamp
  
  if (!timestamp || typeof timestamp !== "string" || !isValidDate(timestamp)) {
    return errorResponse("timestamp field is required")
  }
  
  record["received_at"] = receivedAt.toISOString()
  
  const firehoseRecord = {
    DeliveryStreamName: process.env.FIREHOSE_DELIVERY_STREAM_NAME,
    Record: { 
        Data: Buffer.from(JSON.stringify(record)+'\n')
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

// from https://stackoverflow.com/questions/7445328/check-if-a-string-is-a-date-value
function isValidDate(date) {
  return !!parseDate(date)
}

function parseDate(dateString) {
  const date = new Date(dateString)
  if ((date !== "Invalid Date") && !isNaN(date)) {
    return date
  } else {
    return null
  }
}
