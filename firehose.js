'use strict';

const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const uuidv4 = require('uuid/v4')
const firehose = new AWS.Firehose()
const lambda = new AWS.Lambda()

const shard = require("./shard.js")
const history = require("./history.js")
const naming = require("./naming.js")
const s3utils = require("./s3utils.js")
const customize = require("./customize.js")

// Send the event with the timestamp and project name to firehose
module.exports.sendToFirehose = (projectName, body, receivedAt, log) => {
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

module.exports.unpackFirehose = async function(event, context) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  if (!event.Records || !event.Records.length > 0 || event.Records.some(record => !record.s3 || !record.s3.bucket || !record.s3.bucket.name || !record.s3.object || !record.s3.object.key)) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }

  // list all of the shards so that we can decide which shard to write to
  return naming.listSortedShardsByProjectName().then(sortedShardsByProjectName => {
    // s3 only ever includes one record per event, but this spec allows multiple, so multiple we will process.
    return Promise.all(event.Records.map(s3EventRecord => {
      // process the fire hose file and write results to s3
      return processFirehoseFile(s3EventRecord.s3.bucket.name, s3EventRecord.s3.object.key, sortedShardsByProjectName)
    }))
  }).then(() => {
    // all of the files have been written and meta incoming files have been touched, dispatch the workers to process
    const lambdaArn = naming.getLambdaFunctionArn("dispatchRewardAssignmentWorkers", context.invokedFunctionArn)
    console.log(`invoking dispatchRewardAssignmentWorkers`)

    const params = {
      FunctionName: lambdaArn,
      InvocationType: "Event",
    };
    
    return lambda.invoke(params).promise()
  })
}
 
function processFirehoseFile(s3Bucket, firehoseS3Key, sortedShardsByProjectName) {
  
  let buffersByS3Key = {}
  
  // keep the uuid the same so that events in the same date, project, and shard get mapped to the same key
  const uuidPart = uuidv4()
  
  return s3utils.processCompressedJsonLines(s3Bucket, firehoseS3Key, record => {
    try {
      record.project_name = customize.migrateProjectName(record.project_name || record.api_key) // api_key is deprecated
    } catch (err) {
      console.log(`WARN: skipping record ${JSON.stringify(record)}`, err)
      return;
    }

    // migrate improve v4 records
    if (record.record_type) {
      switch (record.record_type) {
        case "choose":
          // skip old choose records
          return;
        case "using":
          record.type = "decision"
          record.variant = record.properties
          delete record.properties
          break;
        case "rewards":
          record.type = "rewards"
          break;
      }
      delete record.record_type
    }

    if (!record.history_id) {
      record.history_id = record.user_id
      delete record.user_id
    }

    if (!record.project_name) {
      console.log(`WARN: skipping record - no project_name in ${JSON.stringify(record)}`)
      return;
    }

    if (!record.timestamp || !naming.isValidDate(record.timestamp)) {
      console.log(`WARN: skipping record - invalid timestamp in ${JSON.stringify(record)}`)
      return;
    }

    const timestamp = new Date(record.timestamp)
    // client reporting of timestamps in the future are handled in sendToFireHose. This should only happen with some clock skew.
    if (timestamp > Date.now()) {
      console.log(`WARN: timestamp in the future ${JSON.stringify(record)}`)
    }
    
    // ensure everything in history is UTC
    record.timestamp = timestamp.toISOString()

    const projectName = record.project_name;

    // delete project_name from record in case it is sensitive
    delete record.project_name;
    
    if (!naming.isValidProjectName(projectName)) {
      console.log(`WARN: skipping record - invalid project_name, not alphanumeric, underscore, dash, space, period ${JSON.stringify(record)}`)
      return;
    }

    if (!record.history_id) {
      console.log(`WARN: skipping record - no history_id in ${JSON.stringify(record)}`)
      return;
    }
  
    // look at the list of available shards and assign the event to one
    // events are also segmented by event date
    const s3Key = shard.assignToHistoryS3Key(sortedShardsByProjectName[projectName], projectName, record.history_id, record.timestamp, uuidPart)

    let buffers = buffersByS3Key[s3Key]
    if (!buffers) {
      buffers = []
      buffersByS3Key[s3Key] = buffers
    }
    buffers.push(Buffer.from(JSON.stringify(record)+"\n"))

  }).then(() => {
    return writeRecords(buffersByS3Key)
  })
}

function writeRecords(buffersByS3Key) {
  const promises = []
    
  // write out histories
  for (const [s3Key, buffers] of Object.entries(buffersByS3Key)) {
      promises.push(s3utils.compressAndWriteBuffers(s3Key, buffers))
      
      // if its a normal history key (not a variants key) write out the incoming meta file indicating this key should be processed
      if (naming.isHistoryS3Key(s3Key)) {
        promises.push(history.markHistoryS3KeyAsIncoming(s3Key))
      }
    }

  return Promise.all(promises)
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
