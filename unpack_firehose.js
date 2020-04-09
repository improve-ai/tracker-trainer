'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const s3 = new AWS.S3()
const uuidv4 = require('uuid/v4');
const firehose = new AWS.Firehose();
const lambda = new AWS.Lambda()

const naming = require("./naming.js")


// Send the event with the timestamp and project name to firehose
module.exports.sendToFirehose = (projectName, body, receivedAt, log) => {
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

module.exports.unpackFirehose = async function(event, context, cb) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  const promises = []

  if (!event.Records || !event.Records.length > 0) {
    return cb(new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`))
  }

  for (let i = 0; i < event.Records.length; i++) {
    if (!event.Records[i].s3) {
      console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
      continue;
    }

    promises.push(new Promise((resolve, reject) => {

      const buffersByShardIdByProjectName = {}

      const s3Record = event.Records[i].s3

      const gunzip = zlib.createGunzip()

      const stream = s3.getObject({
          Bucket: s3Record.bucket.name,
          Key: s3Record.object.key,
        }).createReadStream()
        .pipe(gunzip)
        .pipe(es.split()) // parse line-by-line
        .pipe(es.mapSync(function(line) {

            // pause the readstream
            stream.pause();

            try {
              if (!line) {
                return;
              }
              let eventRecord = JSON.parse(line)

              if (!eventRecord || !eventRecord.project_name) {
                console.log(`WARN: skipping record - no project_name in ${line}`)
                return;
              }

              if (!eventRecord.user_id) {
                console.log(`WARN: skipping record - no user_id in ${line}`)
                return;
              }
              
              if (!eventRecord.timestamp) {
                console.log(`WARN: skipping record - no timestamp in ${line}`)
                return;
              }
              
              const timestamp = new Date(eventRecord.timestamp)
              
              if (isNaN(timestamp.getTime())) {
                console.log(`WARN: skipping record - invalid timestamp ${line}`)
                return;
              }
              
              // client reporting of timestamps in the future are handled in sendToFireHose. This should only happen with some clock skew.
              if (timestamp > Date.now()) {
                console.log(`WARN: timestamp in the future ${line}`)
              }

              let projectName = eventRecord.project_name;

              // delete project_name from requestRecord in case its sensitive
              delete eventRecord.project_name;

              // allow alphanumeric, underscore, dash, space, period
              if (!projectName.match(/^[\w\- .]+$/i)) {
                console.log(`WARN: skipping record - invalid project_name, not alphanumeric, underscore, dash, space, period ${line}`)
                return;
              }

              let buffersByShardId = buffersByShardIdByProjectName[projectName]
              if (!buffersByShardId) {
                buffersByShardId = {}
                buffersByShardIdByProjectName[projectName] = buffersByShardId
              }

              const shardId = naming.getShardId(eventRecord.user_id);

              let buffers = buffersByShardId[shardId]
              if (!buffers) {
                buffers = []
                buffersByShardId[shardId] = buffers
              }
              buffers.push(Buffer.from(JSON.stringify(eventRecord) + "\n"))
            }
            catch (err) {
              console.log(`error ${err} skipping requestRecord`)
            }
            finally {
              stream.resume();
            }
          })
          .on('error', function(err) {
            console.log('Error while reading file.', err);
            return reject(err)
          })
          .on('end', function() {
            return resolve([s3Record.object.key, buffersByShardIdByProjectName])
          }));
    }))
  }

  return Promise.all(promises).then(results => {

    let promises = []
    for (let [firehoseS3Key, buffersByShardIdByProjectName] of results) { // aggregated by Promise.all
      for (let [projectName, buffersByShardId] of buffersByShardIdByProjectName) {
        for (let [shardId, buffers] of buffersByShardId) {
  
          // sort by timestamp
          buffers.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
          
          // process buffers by splitting it into chunks based on the history file window
          while (buffers.length) {
            const earliestTimestamp = new Date(buffers[0].timestamp)
            let lastIndex = buffers.length-1
            while (new Date(buffers[lastIndex].timestamp) > new Date(earliestTimestamp.getTime() + naming.getHistoryFileWindowMillis())) {
              lastIndex--
            }
            
            const bufferSlice = buffers.slice(0, lastIndex+1)
            buffers = buffers.slice(lastIndex+1, buffers.length)
            if (buffers.length) {
              console.log(`WARN: event timestamps don't all fall within one history file window, splitting`) // this should be rare unless SDKs are queueing events
            }
            
            // the file is named based on the earlest timestamp in the set
            const s3Key = naming.getHistoryS3Key(projectName, shardId, earliestTimestamp, firehoseS3Key)
            
            console.log(`writing ${bufferSlice.length} records to ${s3Key}`)
      
            // write the data
            let params = {
              Body: zlib.gzipSync(Buffer.concat(bufferSlice)),
              Bucket: process.env.RECORDS_BUCKET,
              Key: s3Key
            }
      
            promises.push(s3.putObject(params).promise())
            
            // write the stale file indicating this key should be processed
            params = {
              Body: JSON.stringify({ "key": s3Key }),
              Bucket: process.env.RECORDS_BUCKET,
              Key: naming.getStaleJanitorS3Key(s3Key)
            }
      
            promises.push(s3.putObject(params).promise())
          }
        }
      }
    }
    return Promise.all(promises)
  }).then(results => {
    const params = {
      FunctionName: "lambda-invokes-lambda-node-dev-print_strings",
      InvocationType: "Event",
      Payload: JSON.stringify(message_s)
    };
    
    return lambda.invoke(params).promise()
  })
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
