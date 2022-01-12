'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const DEBUG = process.env.DEBUG
const FIREHOSE_DELIVERY_STREAM_NAME = process.env.FIREHOSE_DELIVERY_STREAM_NAME

const KSUID_REGEX = /^[a-zA-Z0-9]{27}$/

/**
 * Summary. Receives a JSON encoded track protocol record and writes it to the
 * AWS Kinesis Firehose delivery stream.
 */
module.exports.track = async function(event, context) {
  
  if (DEBUG) {
    console.log(JSON.stringify(event))
  }

  const record = JSON.parse(event.body)

  const messageId = record.message_id
  
  if (!isValidKsuid(messageId)) {
    return errorResponse('invalid message_id field')
  }
  
  const timestamp = record.timestamp
  
  if (!isValidDate(timestamp)) {
    return errorResponse('invalid timestamp field')
  }
  
  const type = record.type
  
  if (!isValidType(type)) {
    return errorResponse('invalid type field')
  }
  
  const firehoseRecord = {
    DeliveryStreamName: FIREHOSE_DELIVERY_STREAM_NAME,
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


function isValidType(type) {
  return type && typeof type === 'string' && (type === 'decision' || type === 'reward')
}


function isValidKsuid(id) {
  return id && typeof id === 'string' && KSUID_REGEX.test(id)
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
