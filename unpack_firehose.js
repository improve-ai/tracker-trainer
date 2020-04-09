'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const uuidv4 = require('uuid/v4')
const mmh3 = require('murmurhash3js')
const dateFormat = require('date-format')
const s3 = new AWS.S3()
const lambda = new AWS.Lambda()

const utils = require("./utils.js")

module.exports.unpackFirehose = async function(event, context, cb) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  let now = new Date()
  let filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss", now)
  let uuidPart = uuidv4() // use the same uuid across the files for this unpacking

  let buffersByShardId = {}
  let promises = []

  if (!event.Records || !event.Records.length > 0) {
    return cb(new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`))
  }

  for (let i = 0; i < event.Records.length; i++) {
    if (!event.Records[i].s3) {
      console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
      continue;
    }

    promises.push(new Promise((resolve, reject) => {

      let s3Record = event.Records[i].s3

      let gunzip = zlib.createGunzip()

      let stream = s3.getObject({
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
              
              // FIX check for timestamp in the future

              let projectName = eventRecord.project_name;

              // delete project_name from requestRecord in case its sensitive
              delete eventRecord.project_name;

              // allow alphanumeric, underscore, dash, space, period
              if (!projectName.match(/^[\w\- .]+$/i)) {
                console.log(`WARN: skipping record - invalid project_name, not alphanumeric, underscore, dash, space, period ${line}`)
                return;
              }

              let shardId = getShardId(projectName, eventRecord.user_id, SHARD_COUNT);

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
            return resolve()
          }));
    }))
  }

  return Promise.all(promises).then(results => {
    // ignore results, just use the shared buffersByKey

  let now = new Date()
  let pathDatePart = dateFormat.asString("yyyy/MM/dd/hh", now)
  let filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss",now)
  let uuidPart = uuidv4() // use the same uuid across the files for this unpacking

    let promises = []

    for (var s3Key in buffersByShardId) {
      if (!buffersByShardId.hasOwnProperty(s3Key)) {
        continue
      }

      let buffers = buffersByShardId[s3Key]
      if (!buffers.length) {
        continue;
      }

      console.log(`writing ${buffers.length} records to ${s3Key}`)

      // write the data
      let params = {
        Body: zlib.gzipSync(Buffer.concat(buffers)),
        Bucket: process.env.RECORDS_BUCKET,
        Key: s3Key
      }

      promises.push(s3.putObject(params).promise())
      
      // write the stale file indicating this key should be processed
      params = {
        Body: JSON.stringify({ "key": s3Key }),
        Bucket: process.env.RECORDS_BUCKET,
        Key: utils.getStaleJanitoryS3Key(s3Key)
      }

      promises.push(s3.putObject(params).promise())
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

function getShardId(projectName, userId, shardCount) {
  const bitCount = Math.floor(Math.log2(shardCount))
  // generate a 32 bit bit string so that we can possibly do different subspace queries later
  return mmh3.x86.hash32(projectName.length + ":" + projectName + ":" + userId).toString(2).padStart(32, '0').substring(0, bitCount)
}