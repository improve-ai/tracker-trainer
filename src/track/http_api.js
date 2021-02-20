'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const LOG_PROBABILITY = .1;

module.exports.track = async function(event, context) {
  let receivedAt = new Date()
  
  if (checkShouldLog()) {
    console.log(JSON.stringify(event))
  }

  let record = JSON.parse(event.body)

  if (!record || !record.history_id || !record.message_id) {
    return errorResponse("history_id and message_id fields are required")
  }
  
  record["received_at"] = receivedAt.toISOString()
  // FIX timestamp must never be in the future
  if (!record.timestamp) {
    record["timestamp"] = record["received_at"]
  }
  
/*      if (!record.timestamp || !isValidDate(record.timestamp)) {
      console.log(`WARN: skipping record - invalid timestamp in ${JSON.stringify(record)}`)
      return;
    }

    const timestamp = new Date(record.timestamp)
    // client reporting of timestamps in the future are handled in sendToFireHose. This should only happen with some clock skew.
    if (timestamp > Date.now()) {
      console.log(`WARN: timestamp in the future ${JSON.stringify(record)}`)
    }
    
    // ensure everything in history is UTC
    record.timestamp = timestamp.toISOString()*/

  
  let firehoseRecord = {
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
