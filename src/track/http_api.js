'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const MAX_ID_LENGTH = 256

const DEBUG = process.env.DEBUG

module.exports.track = async function(event, context) {
  if (DEBUG) {
    console.log(JSON.stringify(event))
  }

  const record = JSON.parse(event.body)

  const messageId = record.message_id
  
  if (!isValidId(messageId)) {
    return errorResponse('message_id field is required')
  }
  
  const timestamp = record.timestamp
  
  if (!isValidDate(timestamp)) {
    return errorResponse('timestamp field is required')
  }
  
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
      'Access-Control-Allow-Origin' : '*'
    },
    body: JSON.stringify({ status: 'success' })
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

function isValidId(id) {
  return id && typeof id === 'string' && id.length > 0 && id.length <= MAX_ID_LENGTH
}

// from https://stackoverflow.com/questions/7445328/check-if-a-string-is-a-date-value
function isValidDate(date) {
  return date && typeof date == 'string' && !!parseDate(date)
}

function parseDate(dateString) {
  const date = new Date(dateString)
  if ((date !== 'Invalid Date') && !isNaN(date)) {
    return date
  } else {
    return null
  }
}
