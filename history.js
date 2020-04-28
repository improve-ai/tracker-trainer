'use strict';

const AWS = require('aws-sdk')
const lambda = new AWS.Lambda()
const s3 = new AWS.S3()
const customize = require("./customize.js")
const naming = require("./naming.js")
const shard = require("./shard.js")
const s3utils = require("./s3utils.js")

// dispatch any necessary continued resharding or history processing
// this is called by firehose.js every time a new firehose file is created
// this function is not designed to be executed concurrently, so we set the reservedConcurrency to 1 in serverless.yml
module.exports.dispatchHistoryShardWorkers = async (event, context) => {
  console.log(`processing lambda event ${JSON.stringify(event)}`)
  const reshardLambdaArn = naming.getLambdaFunctionArn("reshard", context.invokedFunctionArn)
  const processHistoryShardLambdaArn = naming.getLambdaFunctionArn("processHistoryShard", context.invokedFunctionArn)

  const projectNames = naming.allProjects()
  return Promise.all(projectNames.map(projectName => {
    // get all of the shard ids & load the shard timestamps
    return Promise.all([naming.listAllShards(projectName), shard.loadAndConsolidateShardLastProcessedDates(projectName)]).then(([shards, shardLastProcessedDates]) => {
      const sortedShards = shards.sort() // sort() modifies shards
      // group the shards
      const [reshardingParents, reshardingChildren, nonResharding] = shard.groupShards(sortedShards)

      // check to see if any of the resharding parents didn't finish and sharding needs to be restarted
      // & check to see if any non-resharding shards have incoming history meta files and haven't been processed recently according to the timestamps
      return Promise.all([shard.dispatchReshardingIfNecessary(reshardLambdaArn, projectName, reshardingParents, shardLastProcessedDates), 
                          dispatchHistoryProcessingIfNecessary(processHistoryShardLambdaArn, projectName, nonResharding, shardLastProcessedDates)])
    })
  }))
}

function dispatchHistoryProcessingIfNecessary(lambdaArn, projectName, nonReshardingShards, lastProcessedDates) {
  const nonReshardingShardsSet = new Set(nonReshardingShards)
  const now = new Date()
  const unix_epoch = new Date(0)
  
  // list all incoming history shards
  return naming.listAllIncomingHistoryShards(projectName).then(incomingShards => {
    return Promise.all(incomingShards.map(shardId => {
      const lastProcessed = lastProcessedDates[shardId] || unix_epoch
  
      // check if the incoming shard isn't currently being resharded and if it hasn't been processed too recently
      if (!nonReshardingShardsSet.has(shardId)) {
        console.log(`skipping project ${projectName} shard ${shardId} for history processing, currently resharding`)
        return 
      }
      
      if ((now - lastProcessed) < process.env.HISTORY_SHARD_REPROCESS_WAIT_TIME_IN_SECONDS * 1000) {
        console.log(`skipping project ${projectName} shard ${shardId} for history processing, last processing ${lastProcessed.toISOString()} was too recent`)
        return
      }

      console.log(`invoking processHistoryShard for project ${projectName} shard ${shardId}`)
  
      const params = {
        FunctionName: lambdaArn,
        InvocationType: "Event",
        Payload: JSON.stringify({"project_name": projectName, "shard_id": shardId, "last_processed_timestamp_updated": true }) // mark that the last processed time is updated so that processHistoryShard doesn't have to also do it
      };
      
      // mark the last processed time as being updated here to have the smallest possible window where multiple redundant workers could be accidentally dispatched
      return Promise.all([shard.updateShardLastProcessed(projectName, shardId), lambda.invoke(params).promise()])
    }))
  })
}

module.exports.processHistoryShard = async function(event, context) {
  
  console.log(`processing event ${JSON.stringify(event)}`)

  const projectName = event.project_name
  const shardId = event.shard_id

  if (!projectName || !shardId) {
    throw new Error(`WARN: missing project_name or shard_id ${JSON.stringify(event)}`)
  }
  
  let updateLastProcessed = Promise.resolve(true)
  // this lambda should only ever be invoked from dispatchHistoryProcessingIfNecessary, but in case
  // some design changes in the future we want to make sure to mark the last processed time is guaranteed to be updated
  if (!event.last_processed_timestamp_updated) {
    updateLastProcessed = shard.updateShardLastProcessed(projectName, shardId)
  }
  
  // list the incoming keys
  return updateLastProcessed.then(() => naming.listAllIncomingHistoryShardS3Keys(projectName, shardId).then(incomingS3Keys => {
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
  }))
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

module.exports.markHistoryS3KeyAsIncoming = (historyS3Key) => {
  if (!naming.isHistoryS3Key(historyS3Key)) {
    throw new Error(`${historyS3Key} must be a history key`)
  }

  const incomingHistoryS3Key = naming.getIncomingHistoryS3Key(historyS3Key)
  console.log(`marking ${incomingHistoryS3Key}`)
  const params = {
    Body: JSON.stringify({ "s3_key": historyS3Key }),
    Bucket: process.env.RECORDS_BUCKET,
    Key: incomingHistoryS3Key
  }

  return s3.putObject(params).promise()
}
