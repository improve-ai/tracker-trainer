'use strict';

const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const customize = require("./customize.js")
const naming = require("./naming.js")
const shard = require("./shard.js")
const s3utils = require("./s3utils.js")

// this function is not designed to be executed concurrently, so we set the reservedConcurrency to 1 in serverless.yml
module.exports.dispatchHistoryShardWorkers = async function(event, context) {
  console.log(`processing event ${JSON.stringify(event)}`)

  // get all of the shard ids
  return naming.listSortedShardsByProjectName().then(sortedShardsByProjectName => {
    // get all of the shard state keys
    return naming.listAllShardTimestampsS3Keys.then(shardTimestampsS3Keys => {
      // load the shard states
      return loadAllShardTimestamps(shardTimestampsS3Keys).then(shardTimestamps => {
        // go through each project
        return Promise.all(Object.entries(sortedShardsByProjectName).map(([projectName, sortedShards]) => {
          // group the shards
          const [reshardingParents, reshardingChildren, nonResharding] = shard.groupShards(sortedShards)
          // check to see if any of the resharding parents didn't finish and sharding needs to be restarted
          // & check to see if any non-resharding shards have incoming history meta files and haven't been processed recently
          return continueReshardingIfNecessary(projectName, reshardingParents, shardTimestamps).then(reshardingUpdatedShardTimestamps => {
            return processHistoryIfNeccessary(nonResharding, reshardingUpdatedShardTimestamps)
          })
        })).then(updatedShardTimestamps => {
          // write a new shard state file and delete the old ones
          return saveShardTimestamps(shardTimestampsS3Keys, updatedShardTimestamps)
        })
      })
    })
  })
}

function continueReshardingIfNecessary(projectName, reshardingParents, ShardTimestamps) {
  // reshard workers have limited reservedConcurrency so may queue for up to 6 hours (plus max 15 minutes of execution) until they should be retried
  // https://docs.aws.amazon.com/lambda/latest/dg/invocation-async.html
  // warn if you frequently see this message you may want to increase reshard worker reservedConconcurrency
  
  return updatedShardTimestamps
}

function processHistoryIfNeccessary(nonResharding) {
  
  return updatedShardTimestamps
}

function loadAllShardTimestamps(s3Keys) {
  return Promise.all(s3Keys.map(s3Key => {
    const params = {
      Bucket: process.env.RECORDS_BUCKET,
      Key: s3Key
    }
    return s3.getObject(params).promise().then(data => {
      return JSON.parse(data.Body.toString('utf-8'))
    })
  })).then(shardTimestampFiles => {
    const consolidatedShardTimestamps = {}
    for (const shardTimestamps of shardTimestampFiles) {
      for (const [shardId, state] of Object.entries(shardTimestamps)) {
        if (!consolidatedShardTimestamps.hasOwnProperty(shardId)) {
          consolidatedShardTimestamps[shardId] = state
        } else {
          // update the state with whichever is newer
          const consolidatedTimestamps = consolidatedShardTimestamps[shardId]
          if (state.last_processed) {
            if (!consolidatedTimestamps.last_processed || new Date(state.last_processed) > new Date(consolidatedTimestamps.last_processed)) {
              consolidatedTimestamps.last_processed = state.last_processed
            }
          }
          if (state.last_resharded) {
            if (!consolidatedTimestamps.last_resharded || new Date(state.last_resharded) > new Date(consolidatedTimestamps.last_resharded)) {
              consolidatedTimestamps.last_resharded = state.last_resharded
            }
          }
        }
      }
    }
    
    return consolidatedShardTimestamps
  })
}

// by saving the consolidated state as a new file with a uuid and then deleting any previous files we can be fairly confident
// that we've rolled up all previously updated state.  In the event that two updates were to happen simultaneously, they would
// be rolled up in the future.
function saveShardTimestamps(s3Keys, shardTimestamps) {
  // create a new unique file that consolidates all current shardState
  let params = {
    Body: JSON.stringify(shardTimestamps),
    Bucket: process.env.RECORDS_BUCKET,
    Key: naming.getShardTimestamps3Key()
  }
  return s3.putObject(params).promise().then(() => {
    // clean up all s3 keys that were consolidated into this new file
    return s3utils.deleteAllKeys(s3Keys)
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

