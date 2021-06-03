'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const MAX_ID_LENGTH = 256

const DEBUG = process.env.DEBUG

module.exports.track = async function(event, context) {
  const receivedAt = new Date()
  
  if (DEBUG) {
    console.log(JSON.stringify(event))
  }

  const record = JSON.parse(event.body)

  const historyId = record.history_id
  
  if (!historyId || typeof historyId !== "string" || historyId.length > MAX_ID_LENGTH) {
    return errorResponse("history_id field is required")
  }
  
  const messageId = record.message_id
  
  if (!messageId || typeof messageId !== "string" || messageId.length > MAX_ID_LENGTH) {
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
  
  return firehose.putRecord(firehoseRecord).promise().then(() => {
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
