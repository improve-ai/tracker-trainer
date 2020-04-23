'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const s3 = new AWS.S3()
const lambda = new AWS.Lambda()
const customize = require("./customize.js")
const naming = require("./naming.js")

const me = module.exports

// this function is not necessarily designed to be executed concurrently, so we set the reservedConcurrency to 1 in serverless.yml
module.exports.dispatchHistoryShardWorkers = async function(event, context) {
  console.log(`processing event ${JSON.stringify(event)}`)

  return naming.listSortedShardsByProjectName().then(sortedShardsByProjectName => {
    for (const [projectName, sortedShards] of Object.entries(sortedShardsByProjectName)) {
      
    }
  })
  
}

// invoke reshardFile lambda for every history and rewarded action S3 key associated with this shard. reshardFile has a maximum reserved concurrency
// so some of the tasks may be queued
function reshard(projectName, shardId) {
  return Promise.all(naming.listAllHistoryShardS3Keys(projectName, shardId), naming.listAllRewardedActionShardS3Keys(projectName, shardId)).then(all => all.flat()).then(s3Keys => {
    return Promise.all(s3Keys.map(s3Key => {
      const lambdaArn = naming.getLambdaFunctionArn("reshardFile", context.invokedFunctionArn)
      console.log(`invoking reshardFile for ${s3Key}`)
    
      const params = {
        FunctionName: lambdaArn,
        InvocationType: "Event",
        Payload: JSON.stringify({"s3_key": s3Key})
      };
      
      return lambda.invoke(params).promise()
    }))
  })
}

// reshard either history or rewarded_action files
module.exports.reshardFile = async function(event, context) {
  console.log(`processing event ${JSON.stringify(event)}`)

  const s3Key = event.s3_key
  
  if (!s3Key) {
    throw new Error(`WARN: missing s3Key ${JSON.stringify(event)}`)
  }
  
  const sortedChildShards = naming.getSortedChildShardsForS3Key(s3Key)
  
  const buffersByS3Key = {}
  // load the data from the key to split
  return me.processCompressedJsonLines(process.env.RECORDS_BUCKET, s3Key, record => {
    if (!record.history_id || !record.timestamp) {
      console.log(`WARN: skipping, missing history_id or timestamp ${JSON.stringify(record)}`)
    }
    
    // pick which child key this record goes to
    const childS3Key = naming.getChildS3Key(s3Key, naming.assignToShard(sortedChildShards, record.history_id))

    const buffers = buffersByS3Key[childS3Key]
    if (!buffers) {
      buffers = []
      buffersByS3Key[childS3Key] = buffers
    }
    buffers.push(Buffer.from(JSON.stringify(record)+"\n"))
  }).then(() => {
    // write out the records
    return Promise.all(Object.entries(buffersByS3Key).map(([s3Key, buffers]) => {
      return me.compressAndWriteBuffers(s3Key, buffers)
    }))
  }).then(() => {
    // delete the parent file
    return deleteKey(s3Key)
  })
}


module.exports.processHistoryShard = async function(event, context) {
  
  // TODO handle presharded incoming meta files

  console.log(`processing event ${JSON.stringify(event)}`)

  const projectName = event.project_name
  const shardId = event.shard_id

  if (!projectName || !shardId) {
    throw new Error(`WARN: missing project_name or shard_id ${JSON.stringify(event)}`)
  }
  
  // naming.list the incoming keys
  return naming.listAllIncomingHistoryShardS3Keys(projectName, shardId).then(incomingS3Keys => {
    console.log(`processing incoming keys: ${JSON.stringify(incomingS3Keys)}`)
    
    // list the entire history for this shard
    return naming.listAllHistoryShardS3Keys(projectName, shardId).then(historyS3Keys => {
      // filter the history by which ones should be newly stale
      const staleS3Keys = naming.filterStaleS3KeysFromIncomingS3Keys(historyS3Keys, incomingS3Keys)
      // mark them stale
      console.log(`creating stale keys: ${JSON.stringify(staleS3Keys)}`)
      return markStale(staleS3Keys).then(result => {
        // incoming has been processed, delete them.
        console.log(`deleting incoming keys: ${JSON.stringify(incomingS3Keys)}`)
        return deleteAllKeys(incomingS3Keys).then(result => {
          return historyS3Keys
        })
      })
    })
  }).then(historyS3Keys => { 
    // list the stale keys for this shard
    return naming.listAllStaleHistoryShardS3Keys(projectName, shardId).then(staleS3Keys => {
      const cache = {}

      for (const staleS3Key in staleS3Keys) {      
        console.log(`processing stale key : ${JSON.stringify(staleS3Key)}`)
    
        const rewardWindowS3Keys = naming.filterWindowS3KeysFromStaleS3Key(historyS3Keys, staleS3Key)
        for (const rewardWindowS3Key of rewardWindowS3Keys) {
          
        }
        return loadHistoryEventsForS3Key(s3Key).then(historyEvents => {
          cache[s3Key] = historyEvents
          
          // TODO separate by user before sorting
          
          historyEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
          // customize may return either a mapping of models -> joined events or a promise that will return the same
          console.log(`assigning rewards to ${JSON.stringify(historyEvents)}`)
          return customize.assignRewardedActionsToModelsFromHistoryEvents(projectName, historyEvents)
        }).then(joinedActionsByModel => {
          console.log(`writing joined events ${JSON.stringify(joinedActionsByModel)}`)
          return writeRewardedActionsByModel(staleS3Key, joinedActionsByModel).then(result => {
            // stale has been processed, delete it
            return deleteKey(staleS3Key)
          })
        })
      }
    })
  })
}

function loadHistoryEventsForS3Keys(s3Keys) {
  let promises = []
  for (let i = 0; i < s3Keys.length; i++) {
    promises.push(loadHistoryEventsForS3Key(s3Keys[i]))
  }
  return Promise.all(promises).then(arraysOfEvents => {
    // Promise.all accumulates an array of results
    return [].concat(...arraysOfEvents) // flatten array
  })
}

function loadHistoryEventsForS3Key(s3Key) {
  let events = []
  
  return new Promise((resolve, reject) => {
    
    let gunzip = zlib.createGunzip()

    console.log(`loadUserEventsForS3Key ${s3Key}`)

    let stream = s3.getObject({ Bucket: process.env.RECORDS_BUCKET, Key: s3Key }).createReadStream().pipe(gunzip).pipe(es.split()).pipe(es.mapSync(function(line) {

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

function writeRewardedActionsByModel(historyS3Key, modelsToJoinedActions) {
  const promises = []
  for (const [modelName, joinedActions] of Object.entries(modelsToJoinedActions)) {
    promises.push(writeRewardedActions(historyS3Key, modelName, joinedActions))
  }
  return Promise.all(promises)
}

/*
 All file name transformations are idempotent to avoid duplicate records.
 projectName and modelName are not included in the fileName to allow files to be copied between models
*/
function writeRewardedActions(historyS3Key, modelName, joinedActions) {
      
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

// get object stream, unpack json lines and process each json object one by one using mapFunction
module.exports.processCompressedJsonLines = (s3Bucket, s3Key, mapFunction) => {
  let results = []
  
  return new Promise((resolve, reject) => {
    
    let gunzip = zlib.createGunzip()

    console.log(`loading ${s3Key} from ${s3Bucket}`)

    let stream = s3.getObject({ Bucket: s3Bucket, Key: s3Key,}).createReadStream().pipe(gunzip).pipe(es.split()).pipe(es.mapSync(function(line) {

      // pause the readstream
      stream.pause();

      try {
        if (!line) {
          return;
        }
        let record = JSON.parse(line)

        if (!record) {
          return;
        }

        results.push(mapFunction(record))
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
      return resolve(results)
    }));
  })
}

module.exports.compressAndWriteBuffers = (s3Key, buffers) => {
  console.log(`writing ${buffers.length} records to ${s3Key}`)
  
  let params = {
    Body: zlib.gzipSync(Buffer.concat(buffers)),
    Bucket: process.env.RECORDS_BUCKET,
    Key: s3Key
  }

  return s3.putObject(params).promise()
}

function deleteAllKeys(s3Keys) {
  const promises = []
  for (const s3Key of s3Keys) {
    promises.push(deleteKey(s3Key))
  }
  return Promise.all(promises)
}

function deleteKey(s3Key) {
  let params = {
    Bucket: process.env.RECORDS_BUCKET,
    Key: s3Key
  }
  return s3.deleteObject(params).promise()
}

