'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const s3 = new AWS.S3()
const uuidv4 = require('uuid/v4');
const firehose = new AWS.Firehose();
const lambda = new AWS.Lambda()

const customize = require("./customize.js")
const naming = require("./naming.js")
const utils = require("./utils.js")

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

module.exports.unpackFirehose = async function(event, context) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  if (!event.Records || !event.Records.length > 0 || event.Records.some(record => !record.s3 || !record.s3.bucket || !record.s3.bucket.name || !record.s3.object || !record.s3.object.key)) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }

  // list all of the shards so that we can decide which shard to write to
  return listShardsByProjectName().then(shardsByProjectName => {
    console.log(`shardsByProjectName ${JSON.stringify(shardsByProjectName)}`)
    // s3 only ever includes one record per event, but this spec allows multiple, so multiple we will process.
    return Promise.all(event.Records.map(s3EventRecord => {
      return processFirehoseFile(s3EventRecord.s3.bucket.name, s3EventRecord.s3.object.key, shardsByProjectName)
    }))
  })
}
 
function processFirehoseFile(s3Bucket, s3Key, shardsByProjectName) {
  return new Promise((resolve, reject) => {

    const eventsByShardIdByProjectName = {}
    const variantRecordsByModelByProjectName = {}

    const gunzip = zlib.createGunzip()

    const stream = s3.getObject({
        Bucket: s3Bucket,
        Key: s3Key,
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

          if (eventRecord.record_type === "choose") { // DEPRECATED
            console.log(`WARN: skipping choose record`)
            return;
          }

          if (!eventRecord || !eventRecord.project_name) {
            console.log(`WARN: skipping record - no project_name in ${line}`)
            return;
          }

          if (!eventRecord.timestamp || !naming.isValidDate(eventRecord.timestamp)) {
            console.log(`WARN: skipping record - invalid timestamp in ${line}`)
            return;
          }

          // client reporting of timestamps in the future are handled in sendToFireHose. This should only happen with some clock skew.
          if (new Date(eventRecord.timestamp) > Date.now()) {
            console.log(`WARN: timestamp in the future ${line}`)
          }

          let projectName = eventRecord.project_name;

          // delete project_name from requestRecord in case its sensitive
          delete eventRecord.project_name;
          
          if (!naming.isValidProjectName(projectName)) {
            console.log(`WARN: skipping record - invalid project_name, not alphanumeric, underscore, dash, space, period ${line}`)
            return;
          }
          
          // Handle variants records
          if (eventRecord.type && eventRecord.type === "variants") {
            if (!eventRecord.model) {
              console.log(`WARN: skipping record - missing model for variants type ${line}`)
              return
            }

            if (!naming.isValidModelName(eventRecord.model)) {
              console.log(`WARN: skipping record - invalid model name, not alphanumeric, underscore, dash, space, period ${line}`)
              return;
            }
            
            let variantRecordsByModel = variantRecordsByModelByProjectName[projectName]
            if (!variantRecordsByModel) {
              variantRecordsByModel = {}
              variantRecordsByModelByProjectName[projectName] = variantRecordsByModel
            }
            
            let variantRecords = variantRecordsByModel[eventRecord.model]
            if (!variantRecords) {
              variantRecords = []
              variantRecordsByModel[eventRecord.model] = variantRecords
            }
            variantRecords.push(eventRecord)
            
            return
          }
          
          // user_id is deprecated
          // history_id is not required for variants records
          if (!eventRecord.user_id && !eventRecord.history_id) {
            console.log(`WARN: skipping record - no history_id in ${line}`)
            return;
          }
          
          if (!eventRecord.history_id) {
            eventRecord.history_id = eventRecord.user_id
          }

          // Handle all other events
          let eventsByShardId = eventsByShardIdByProjectName[projectName]
          if (!eventsByShardId) {
            eventsByShardId = {}
            eventsByShardIdByProjectName[projectName] = eventsByShardId
          }

          const shardId = naming.getShardId(eventRecord.history_id);

          let events = eventsByShardId[shardId]
          if (!events) {
            events = []
            eventsByShardId[shardId] = events
          }
          events.push(eventRecord)
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
        return resolve([s3Key, eventsByShardIdByProjectName, variantRecordsByModelByProjectName])
      }));
  }).then((firehoseS3Key, eventsByShardIdByProjectName, variantRecordsByModelByProjectName) => {

    let promises = []
    const staleShardsByProjectName = {}
    
    // write out variants
    for (const [projectName, variantRecordsByModel] of Object.entries(variantRecordsByModelByProjectName)) {
      for (const [modelName, variantRecords] of Object.entries(variantRecordsByModel)) {
        
          const s3Key = naming.getVariantsS3Key(projectName, modelName, firehoseS3Key)
          
          console.log(`writing ${variantRecords.length} records to ${s3Key}`)
          
          // write the data
          let params = {
            Body: zlib.gzipSync(Buffer.concat(variantRecords.map(event => Buffer.from(JSON.stringify(event) + "\n")))),
            Bucket: process.env.RECORDS_BUCKET,
            Key: s3Key
          }
    
          promises.push(s3.putObject(params).promise())
      }
    }
    
    // write out histories
    for (let [projectName, eventsByShardId] of Object.entries(eventsByShardIdByProjectName)) {
      for (let [shardId, events] of Object.entries(eventsByShardId)) {

        // keep track of the stale shards to kick off history processing workers
        let staleShards = staleShardsByProjectName[projectName]
        if (!staleShards) {
          staleShards = new Set()
          staleShardsByProjectName[projectName] = staleShards
        }
        staleShards.add(shardId)
        
        // sort by timestamp
        events.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        
        // process events by splitting it into chunks based on the history file window
        while (events.length) {
          const earliestTimestamp = new Date(events[0].timestamp)
          let lastIndex = events.length-1
          while (new Date(events[lastIndex].timestamp) > new Date(earliestTimestamp.getTime() + naming.getHistoryFileWindowMillis())) {
            lastIndex--
          }
          
          const eventsSlice = events.slice(0, lastIndex+1)
          events = events.slice(lastIndex+1, events.length)
          if (events.length) {
            console.log(`WARN: event timestamps don't all fall within one history file window, splitting`) // this should be rare unless SDKs are queueing events
          }
          
          // the file is named based on the earlest timestamp in the set
          const s3Key = naming.getHistoryS3Key(projectName, shardId, earliestTimestamp, firehoseS3Key)
          
          console.log(`writing ${eventsSlice.length} records to ${s3Key}`)
          
          // write the data
          let params = {
            Body: zlib.gzipSync(Buffer.concat(eventsSlice.map(event => Buffer.from(JSON.stringify(event) + "\n")))),
            Bucket: process.env.RECORDS_BUCKET,
            Key: s3Key
          }
    
          promises.push(s3.putObject(params).promise())
          
          // write the incoming meta file indicating this key should be processed
          params = {
            Body: JSON.stringify({ "key": s3Key }),
            Bucket: process.env.RECORDS_BUCKET,
            Key: naming.getIncomingHistoryS3Key(s3Key)
          }
    
          promises.push(s3.putObject(params).promise())
        }
      }
    }
  
    return Promise.all(promises).then(results => {
      return staleShardsByProjectName
    })
  }).then(staleShardsByProjectName => {
    // TODO list all meta files to find really old stale ones.  Can probably get rid of the staleShardsByProjectName and just use the list results
    const promises = []
    for (const [projectName, staleShards] of Object.entries(staleShardsByProjectName)) {
      for (const staleShard of staleShards) {
        const lambdaArn = naming.getLambdaFunctionArn("processHistoryShard", context.invokedFunctionArn)
        console.log(`invoking processHistoryShard for project ${projectName} shard ${staleShard}`)
    
        const params = {
          FunctionName: lambdaArn,
          InvocationType: "Event",
          Payload: JSON.stringify({project_name: projectName, shard_id: staleShard})
        };
        
        promises.push(lambda.invoke(params).promise())
      }
    }
    return Promise.all(promises)
  })
}

function listShardsByProjectName() {
  const projectNames = Object.keys(customize.getProjectNamesToModelNamesMapping())
  return Promise.all(projectNames.map(projectName => {
    console.log(`listing shards for project ${projectName}`)
    const params = {
      Bucket: process.env.RECORDS_BUCKET,
      Delimiter: '/',
      Prefix: naming.getHistoryS3KeyPrefix(projectName)
    }
    
    return listAllCommonPrefixes(params).then(shardIds => {
      console.log(`shardIds ${JSON.stringify(shardIds)}`)
      return [projectName, shardIds]
    })
  })).then(projectNamesAndShardIds => {
    const shardsByProjectNames = {}
    for (const [projectName, shardIds] of projectNamesAndShardIds) {
      shardsByProjectNames[projectName] = shardIds
    }
    return shardsByProjectNames
  })
}

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
const listAllCommonPrefixes = (params, out = []) => new Promise((resolve, reject) => {
  s3.listObjectsV2(params).promise()
    .then(({CommonPrefixes, IsTruncated, NextContinuationToken}) => {
      out.push(...CommonPrefixes.map(o => o.Prefix.split('/').slice(-2)[0])); // split and grab the second to last item from the Prefix
      !IsTruncated ? resolve(out) : resolve(listAllCommonPrefixes(Object.assign(params, {ContinuationToken: NextContinuationToken}), out));
    })
    .catch(reject);
});


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
