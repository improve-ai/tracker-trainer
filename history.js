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

  // FIX may have to relist to get all stale files for this shard

  if (!event.s3Key) {
    throw new Error(`WARN: missing s3Key ${JSON.stringify(event)}`)
  }

  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: naming.getHistoryShardS3KeyPrefix(event.s3Key)
  }
  
  const projectName = naming.getProjectNameFromHistoryS3Key(event.s3Key)
  
  // FIX scope to just the necessary files by date
  
  return listAllKeys(params).then(s3Keys => {
    console.log(`s3Keys : ${JSON.stringify(s3Keys)}`)
    return loadUserEventsForS3Keys(s3Keys)
  }).then(userEvents => {
    userEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
    // customize may return either a mapping of models -> joined events or a promise that will return the same
    console.log(`assigning rewards to ${JSON.stringify(userEvents)}`)
    return customize.assignRewards(projectName, userEvents)
  }).then(joinedEventsByModel => {
    console.log(`writing joined events ${JSON.stringify(joinedEventsByModel)}`)
    return writeJoinedActionsByModel(event.s3Key, joinedEventsByModel)
  })
}

function loadUserEventsForS3Keys(s3Keys) {
  let promises = []
  for (let i = 0; i < s3Keys.length; i++) {
    promises.push(loadUserEventsForS3Key(s3Keys[i]))
  }
  return Promise.all(promises).then(arraysOfEvents => {
    // Promise.all accumulates an array of results
    return [].concat(...arraysOfEvents) // flatten array
  })
}

function loadUserEventsForS3Key(s3Key) {
  let events = []
  
  return new Promise((resolve, reject) => {
    
    let gunzip = zlib.createGunzip()

    console.log(`loadUserEventsForS3Key ${s3Key}`)

    let stream = s3.getObject({ Bucket: process.env.RECORDS_BUCKET, Key: s3Key,}).createReadStream().pipe(gunzip).pipe(es.split()).pipe(es.mapSync(function(line) {

      // pause the readstream
      stream.pause();

      try {
        if (!line) {
          return;
        }
        let eventRecord = JSON.parse(line)

        if (!eventRecord || !eventRecord.timestamp || !naming.isValidDate(eventRecord.timestamp)) {
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

function writeJoinedActionsByModel(historyS3Key, modelsToJoinedEvents) {
  const promises = []
  for (const [modelName, joinedActions] of Object.entries(modelsToJoinedEvents)) {
    promises.push(writeJoinedActions(historyS3Key, modelName, joinedActions))
  }
  return Promise.all(promises)
}

/*
 All file name transformations are idempotent to avoid duplicate records.
 projectName and modelName are not included in the fileName to allow files to be copied between models
*/
function writeJoinedActions(historyS3Key, modelName, joinedActions) {
      
      // allow alphanumeric, underscore, dash, space, period
      if (!naming.isValidModelName(modelName)) {
        console.log(`WARN: skipping ${joinedActions.length} records - invalid modelName, not alphanumeric, underscore, dash, space, period ${modelName}`)
        return;
      }
      
      let jsonLines = ""
      for (const joinedEvent of joinedActions) {
        jsonLines += (JSON.stringify(joinedEvent) + "\n")
      }
      
      let s3Key = naming.getJoinedS3Key(historyS3Key, modelName)
      console.log(`writing ${joinedActions.length} records to ${s3Key}`)
        
      let params = {
        Body: zlib.gzipSync(Buffer.from(jsonLines)),
        Bucket: process.env.RECORDS_BUCKET,
        Key: s3Key
      }
      return s3.putObject(params).promise()
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
