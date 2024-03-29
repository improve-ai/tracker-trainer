'use strict';

const AWS = require('aws-sdk');
const firehose = new AWS.Firehose();

const DEBUG = process.env.DEBUG
const FIREHOSE_DELIVERY_STREAM_NAME = process.env.FIREHOSE_DELIVERY_STREAM_NAME

const KSUID_REGEX = /^[a-zA-Z0-9]{27}$/

/**
 * Summary. Receives a JSON encoded track protocol record and writes it to the
 * AWS Kinesis Firehose delivery stream. The max record size is 1000 KiB 
 * (max firehose record size) - 1 byte for the newline.
 */
module.exports.track = async function(event, context) {
  
  if (DEBUG) {
    console.log(JSON.stringify(event))
  }

  const record = JSON.parse(event.body)

  if (!isValidKsuid(record.message_id)) {
    return errorResponse('invalid message_id field')
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
    body: JSON.stringify({ error: { message: message } })
  }
  return response
}


function isValidKsuid(id) {
  return id && typeof id === 'string' && KSUID_REGEX.test(id)
}
