'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const mmh3 = require('murmurhash3js')
const s3 = new AWS.S3()
const lambda = new AWS.Lambda()
const customize = require("./customize.js")
const naming = require("./naming.js")

module.exports.dispatchRewardProcessingWorkers = async function(event, context) {
  
  listObjectsV2
  // Key: "history/janitor/stale/"+s3Key.substring("history/events/".length)
  
     const params = {
      FunctionName: "lambda-invokes-lambda-node-dev-print_strings",
      InvocationType: "Event",
      Payload: JSON.stringify(message_s)
    };
    
    return lambda.invoke(params).promise()
}

module.exports.processHistoryShard = async function(event, context) {

  console.log(`processing event ${JSON.stringify(event)}`)

  // may have to relist to get all stale files for this shard

  if (!event.s3Key) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }

  // histories/projectName/hashedUserId/improve-v5-pending-events-projectName-hashedUserId-yyyy-MM-dd-hh-mm-ss-uuid.gz
  // histories/events/projectName/shardCount/shardId/yyyy/MM/dd/hh/improve-events-projectName-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz

  const [projectName, hashedUserId] = s3Record.object.key.split('/').slice(1,3)
  const s3Bucket = s3Record.bucket.name
  
  return listAllKeys({ Bucket: s3Bucket, Prefix: `histories/${projectName}/${hashedUserId}`}).then(s3Keys => {
    console.log(`s3Keys : ${JSON.stringify(s3Keys)}`)
    return loadUserEventsForS3Keys(s3Bucket, s3Keys)
  }).then(userEvents => {
    userEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
    // customize may return either a mapping of models -> joined events or a promise that will return the same
    console.log(`assigning rewards to ${JSON.stringify(userEvents)}`)
    return customize.assignRewards(userEvents)
  }).then(joinedEventsByModel => {
    console.log(`writing joined events ${JSON.stringify(joinedEventsByModel)}`)
    return writeJoinedEventsByModel(projectName, fileName, joinedEventsByModel)
  })
}

function loadUserEventsForS3Keys(s3Bucket, s3Keys) {
  let promises = []
  for (let i = 0; i < s3Keys.length; i++) {
    promises.push(loadUserEventsForS3Key(s3Bucket, s3Keys[i]))
  }
  return Promise.all(promises).then(arraysOfEvents => {
    // Promise.all accumulates an array of results
    return [].concat(...arraysOfEvents) // flatten array
  })
}

function loadUserEventsForS3Key(s3Bucket, s3Key) {
  let events = []
  
  return new Promise((resolve, reject) => {
    
    let gunzip = zlib.createGunzip()

    console.log(`loadUserEventsForS3Key s3Bucket: ${s3Bucket} s3Key : ${s3Key}`)

    let stream = s3.getObject({ Bucket: s3Bucket, Key: s3Key,}).createReadStream().pipe(gunzip).pipe(es.split()).pipe(es.mapSync(function(line) {

      // pause the readstream
      stream.pause();

      try {
        if (!line) {
          return;
        }
        let eventRecord = JSON.parse(line)

        if (!eventRecord || !eventRecord.timestamp || !isDate(eventRecord.timestamp)) {
          console.log(`WARN: skipping record - no timestamp in ${line}`)
          return;
        }

        events.push(eventRecord)
      } catch (err) {
        console.log(`error ${err} skipping record`)
      } finally {
        stream.resume();
      }
    })
    .on('error', function(err) {
      console.log('Error while reading file.', err);
      return reject(err)
    })
    .on('end', function() {
      return resolve(events)
    }));
  })
}

function writeJoinedEventsByModel(projectName, fileName, modelsToJoinedEvents) {
  const promises = []
  for (const [modelName, joinedEvents] of Object.entries(modelsToJoinedEvents)) {
    promises.push(writeJoinedEvents(projectName, modelName, fileName, joinedEvents))
  }
  return Promise.all(promises)
}

/*
 All file name transformations are idempotent to avoid duplicate records.
 projectName and modelName are not included in the fileName to allow files to be copied between models
*/
function writeJoinedEvents(projectName, modelName, fileName, joinedEvents) {
      
      let jsonLines = ""
      for (const joinedEvent of joinedEvents) {
        jsonLines += (JSON.stringify(joinedEvent) + "\n")
      }
      
      // joined/data/projectName/modelName/shardCount/(train|validation)/yyyy/MM/dd/hh/improve-joined-projectName-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz

      let s3Key = `joined/${projectName}/${modelName}/${getTrainValidationPathPart(fileName)}/${fileName}`
      console.log(`writing ${joinedEvents.length} records to ${s3Key}`)
        
      let params = {
        Body: zlib.gzipSync(Buffer.from(jsonLines)),
        Bucket: process.env.RECORDS_BUCKET,
        Key: s3Key
      }
      return s3.putObject(params).promise()
}

/* 
 Implements the train/validation split based on the hash of the file name.  
 All file name transformations are idempotent to avoid duplicate records.
 projectName and modelName are not included in the hash to allow files to be copied between models
*/
function getTrainValidationPathPart(fileName) {
  let validationProportion = parseFloat(process.env.VALIDATION_PROPORTION)
  
  console.log(`mmh32 ${mmh3.x86.hash32(fileName)}`)
  if (mmh3.x86.hash32(fileName) / (2.0 ** 32) < validationProportion) {
    return naming.getValidationPathPart()
  } else {
    return naming.getTrainPathPart()
  }
}

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
const listAllKeys = (params, out = []) => new Promise((resolve, reject) => {
  s3.listObjectsV2(params).promise()
    .then(({Contents, IsTruncated, NextContinuationToken}) => {
      out.push(...Contents.map(o => o.Key));
      !IsTruncated ? resolve(out) : resolve(listAllKeys(Object.assign(params, {ContinuationToken: NextContinuationToken}), out));
    })
    .catch(reject);
});

// from https://stackoverflow.com/questions/7445328/check-if-a-string-is-a-date-value
function isDate(date) {
  return (new Date(date) !== "Invalid Date") && !isNaN(new Date(date));
}
