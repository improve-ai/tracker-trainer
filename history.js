'use strict';

const AWS = require('aws-sdk')
const zlib = require('zlib')
const es = require('event-stream')
const s3 = new AWS.S3()
const customize = require("./customize.js")
const naming = require("./naming.js")

module.exports.dispatchHistoryShardWorkers = async function(event, context) {
  console.log(`processing event ${JSON.stringify(event)}`)

  // list all of the shards so that we can decide which shard to write to
  return module.exports.listSortedShardsByProjectName().then(sortedShardsByProjectName => {
    for (const [projectName, sortedShards] of Object.entries(sortedShardsByProjectName)) {
      
    }
  })
}

module.exports.reshardFile = async function(event, context) {
  console.log(`processing event ${JSON.stringify(event)}`)

  const s3Key = event.s3Key
  
  if (!s3Key) {
    throw new Error(`WARN: missing s3Key ${event}`)
  }
}



module.exports.processHistoryShard = async function(event, context) {

  console.log(`processing event ${JSON.stringify(event)}`)

  const projectName = event.project_name
  const shardId = event.shard_id

  if (!projectName || !shardId) {
    throw new Error(`WARN: missing project_name or shard_id ${JSON.stringify(event)}`)
  }
  
  // list the incoming keys
  return listAllIncomingHistoryShardS3Keys(projectName, shardId).then(incomingS3Keys => {
    console.log(`processing incoming keys: ${JSON.stringify(incomingS3Keys)}`)
    
    // list the entire history for this shard
    return listAllHistoryShardS3Keys(projectName, shardId).then(historyS3Keys => {
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
    return listAllStaleHistoryShardS3Keys(projectName, shardId).then(staleS3Keys => {
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
          return writeJoinedActionsByModel(staleS3Key, joinedActionsByModel).then(result => {
            // stale has been processed, delete it
            return deleteKey(staleS3Key)
          })
        })
      }
    })
  })
}

function listAllIncomingHistoryShardS3Keys(projectName, shardId) {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: naming.getIncomingHistoryShardS3KeyPrefix(projectName, shardId)
  }

  return listAllKeys(params)
}

function listAllHistoryShardS3Keys(projectName, shardId) {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: naming.getHistoryShardS3KeyPrefix(projectName, shardId)
  }
  
  return listAllKeys(params)
}

function markStale(s3Keys) {
  const promises = []
  for (const s3Key of s3Keys) {
    // write the incoming meta file indicating this key should be processed
    const params = {
      Body: JSON.stringify({ "key": s3Key }),
      Bucket: process.env.RECORDS_BUCKET,
      Key: naming.getStaleHistoryS3Key(s3Key)
    }

    promises.push(s3.putObject(params).promise())
  }
  
  return Promise.all(promises)
}

function listAllStaleHistoryShardS3Keys(projectName, shardId) {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: naming.getStaleHistoryShardS3KeyPrefix(projectName, shardId)
  }

  return listAllKeys(params)
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

function writeJoinedActionsByModel(historyS3Key, modelsToJoinedActions) {
  const promises = []
  for (const [modelName, joinedActions] of Object.entries(modelsToJoinedActions)) {
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

module.exports.listSortedShardsByProjectName = () => {
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
      shardIds.sort() // sort the shards
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

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
const listAllKeys = (params, out = []) => new Promise((resolve, reject) => {
  s3.listObjectsV2(params).promise()
    .then(({Contents, IsTruncated, NextContinuationToken}) => {
      out.push(...Contents.map(o => o.Key));
      !IsTruncated ? resolve(out) : resolve(listAllKeys(Object.assign(params, {ContinuationToken: NextContinuationToken}), out));
    })
    .catch(reject);
});
