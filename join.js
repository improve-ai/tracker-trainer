'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const s3 = new AWS.S3()
const customize = require("./customize.js")

module.exports.join = function(event, context, cb) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  let promises = []

  if (!event.Records || !event.Records.length > 0) {
    return cb(new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`))
  }

  for (let i = 0; i < event.Records.length; i++) {
    if (!event.Records[i].s3) {
      console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
      continue;
    }

    const s3Record = event.Records[i].s3

    // histories/projectName/hashedUserId/improve-v5-pending-events-projectName-hashedUserId-yyyy-MM-dd-hh-mm-ss-uuid.gz
    const [projectName, hashedUserId] = s3Record.object.key.split('/').slice(1,3)
    const s3Bucket = s3Record.bucket.name
    
    promises.push(listAllKeys({ Bucket: s3Bucket, Prefix: `histories/${projectName}/${hashedUserId}`}).then(s3Keys => {
      console.log(`s3Keys : ${JSON.stringify(s3Keys)}`)
      return loadUserEventsForS3Keys(s3Bucket, s3Keys)
    }).then(userEvents => {
      userEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
      // customize may return either a mapping of models -> joined events or a promise that will return the same
      console.log(`assigning rewards to ${JSON.stringify(userEvents)}`)
      return customize.assignRewards(userEvents)
    }).then(modelsToJoinedEvents => {
      console.log(`writing joined events ${JSON.stringify(modelsToJoinedEvents)}`)
      return writeModelsToJoinedEvents(projectName, hashedUserId, modelsToJoinedEvents)
    }))
  }

  // Really there should just be just one promise in promises because S3 events are
  // one key at a time, but the format does allow multiple event Records
  return Promise.all(promises).then(results => {
    return cb(null, 'success')
  }, err => {
    return cb(err)
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

function writeModelsToJoinedEvents(projectName, hashedUserId, modelsToJoinedEvents) {
  const promises = []
  for (const [modelName, joinedEvents] of Object.entries(modelsToJoinedEvents)) {
    promises.push(writeJoinedEvents(projectName, modelName, hashedUserId, joinedEvents))
  }
  return Promise.all(promises)
}

function writeJoinedEvents(projectName, modelName, hashedUserId, joinedEvents) {
      
      let jsonLines = ""
      for (const joinedEvent of joinedEvents) {
        jsonLines += (JSON.stringify(joinedEvent) + "\n")
      }
  
      let s3Key = `joined/${projectName}/${modelName}/${hashedUserId}.gz`    
      console.log(`writing ${joinedEvents.length} records to ${s3Key}`)
        
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

// from https://stackoverflow.com/questions/7445328/check-if-a-string-is-a-date-value
function isDate(date) {
  return (new Date(date) !== "Invalid Date") && !isNaN(new Date(date));
}
