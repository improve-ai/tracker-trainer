'use strict';

const AWS = require('aws-sdk')
const lambda = new AWS.Lambda()
const s3 = new AWS.S3()
const _ = require('lodash')
const naming = require("./naming.js")
const shard = require("./shard.js")
const s3utils = require("./s3utils.js")
const customize = require("./customize.js")

// dispatch any necessary continued resharding or history processing
// this is called by firehose.js every time a new firehose file is created
// this function is not designed to be executed concurrently, so we set the reservedConcurrency to 1 in serverless.yml
module.exports.dispatchHistoryShardWorkers = async (event, context) => {
  console.log(`processing lambda event ${JSON.stringify(event)}`)

  const projectNames = naming.allProjects()
  return Promise.all(projectNames.map(projectName => {
    // get all of the shard ids & load the shard timestamps
    return Promise.all([naming.listAllShards(projectName), shard.loadAndConsolidateShardLastProcessedDates(projectName)]).then(([shards, shardLastProcessedDates]) => {
      const sortedShards = shards.sort() // sort() modifies shards
      // group the shards
      const [reshardingParents, reshardingChildren, nonResharding] = shard.groupShards(sortedShards)
      console.log(`project ${projectName} has ${shards.length-nonResharding.length}/${shards.length} shards currently resharding`)
      // check to see if any of the resharding parents didn't finish and sharding needs to be restarted
      // & check to see if any non-resharding shards have incoming history meta files and haven't been processed recently according to the timestamps
      return Promise.all([shard.dispatchReshardingIfNecessary(context, projectName, reshardingParents, shardLastProcessedDates), 
                          dispatchHistoryProcessingIfNecessary(context, projectName, nonResharding, shardLastProcessedDates)])
    })
  }))
}

function dispatchHistoryProcessingIfNecessary(lambdaContext, projectName, nonReshardingShards, lastProcessedDates) {
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
        FunctionName: naming.getLambdaFunctionArn("processHistoryShard", lambdaContext.invokedFunctionArn),
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

  // list the incoming keys and history keys for this shard
  return updateLastProcessed.then(() => Promise.all([naming.listAllHistoryShardS3KeysMetadata(projectName, shardId), naming.listAllIncomingHistoryShardS3Keys(projectName, shardId)]).then(([historyS3KeysMetadata, incomingHistoryS3Keys]) => {
    const staleS3KeysMetadata = filterStaleHistoryS3KeysMetadata(historyS3KeysMetadata, incomingHistoryS3Keys)

    // check size of the keys to be re-processed and reshard if necessary
    const totalSize = staleS3KeysMetadata.reduce((acc, cur) => acc+cur.Size,0)
    console.log(`${totalSize} bytes of stale history data for project ${projectName} shard ${shardId}`)
    if (totalSize > (process.env.HISTORY_SHARD_WORKER_MAX_PAYLOAD_IN_MB * 1024 * 1024)) {
      console.log(`resharding project ${projectName} shard ${shardId} - stale history data is too large`)
      return shard.invokeReshardLambda(context, projectName, shardId)
    }
    
    return loadAndConsolidateHistoryRecords(staleS3KeysMetadata.map(o => o.Key)).then(staleHistoryRecords => {
      
      // perform customize before we access the records
      staleHistoryRecords = customize.modifyHistoryRecords(projectName, staleHistoryRecords)

      // group history records by history id
      const historyRecordsByHistoryId = Object.fromEntries(Object.entries(_.groupBy(staleHistoryRecords, 'history_id')))

      // convert history records into rewarded actions
      let rewardedActions = []
      
      for (const [historyId, historyRecords] of Object.entries(historyRecordsByHistoryId)) {
        rewardedActions = rewardedActions.concat(getRewardedActionsForHistoryRecords(projectName, historyId, historyRecords))
      }

      return writeRewardedActions(projectName, shardId, rewardedActions)      
    }).then(() => {
      // incoming has been processed, delete them.
      return s3utils.deleteAllKeys(incomingHistoryS3Keys)
    })
  }))
}

// TODO filter
function filterStaleHistoryS3KeysMetadata(historyS3Keys, incomingHistoryS3Keys) {
  // TODO log the dates that we're looking at
  // Oh crap, we can't process actions after date X if we're processing in the middle of a window. So if we receive some old events
  // we need to grab the entire window for those old events and ONLY process those old events.
  return historyS3Keys
}

function loadAndConsolidateHistoryRecords(historyS3Keys) {
  // group the keys by path
  return Promise.all(Object.entries(naming.groupHistoryS3KeysByDatePath(historyS3Keys)).map(([path, pathS3Keys]) => {
      // load all records from the s3 keys for this specific path
      const messageIds = new Set()
      let duplicates = 0
      return Promise.all(pathS3Keys.map(s3Key => s3utils.processCompressedJsonLines(process.env.RECORDS_BUCKET, s3Key, (record) => {
        // check for duplicate message ids.  This is most likely do to re-processing firehose files multiple times.
        const messageId = record.message_id
        if (!messageId || messageIds.has(messageId)) {
          duplicates++
          return null
        } else {
          messageIds.add(messageId)
          return record
        }
      }))).then(results => results.flat()).then(records => {
        if (duplicates) {
          console.log(`ignoring ${duplicates} records with missing or duplicate message_id fields`)
        }
        // TODO remove
        console.log(`rough records size: ${JSON.stringify(records).length} bytes`)
        if (pathS3Keys.length == 1) {
          return records
        }

        console.log(`consolidating ${pathS3Keys.length} files at ${path} into 1 file`)
        const buffers = records.map(record => Buffer.from(JSON.stringify(record)+"\n"))
        return s3utils.compressAndWriteBuffers(naming.getConsolidatedHistoryS3Key(pathS3Keys[0]), buffers).then(() => {
          s3utils.deleteAllKeys(pathS3Keys)
        }).then(() => records)
      })
  })).then(results => results.flat())
}

// throw an Error on any parsing problems or customize bugs, causing the whole historyId to be skipped
// TODO parsing problems should be massaged before this point so that the only possible source of parsing bugs
// is customize calls.
function getRewardedActionsForHistoryRecords(projectName, historyId, historyRecords) {
  // TODO ignore duplicate message ids
  
  const actionRecords = []
  const rewardsRecords = []
  for (const historyRecord of historyRecords) {
    // make sure the history ids weren't modified in customize
    if (historyId !== historyRecord.history_id) {
      // (this might not be a big deal since we're using shard Id to write)
      throw new Error(`historyId ${historyId} does not match record ${JSON.stringify(historyRecord)}`)
    }
    
    // grab all the values that we don't want to change before during further calls to customize
    const timestamp = historyRecord.timestamp
    const timestampDate = new Date(timestamp)
    if (!timestamp || isNaN(timestampDate.getTime())) {
      throw new Error(`invalid timestamp for history record ${JSON.stringify(historyRecord)}`)
    }
    const messageId = historyRecord.message_id
    if (!messageId || !_.isString(messageId)) {
      throw new Error(`invalid message_id for history record ${JSON.stringify(historyRecord)}`)
    }

    let inferredActionRecords; // may remain null or be an array of actionRecords
    
    // the history record may be of type "action", in which case it itself is an action record
    if (historyRecord.type === "action") {
      inferredActionRecords = [historyRecord]
    }
    
    // the history record may have attached "actions"
    if (historyRecord.actions) {
      if (!Array.isArray(historyRecord.actions)) {
        throw new Error(`attached actions must be array type ${JSON.stringify(historyRecord)}`)
      } 
      
      if (!inferredActionRecords) {
        inferredActionRecords = historyRecord.actions
      } else {
        inferredActionRecords.concat(historyRecord.actions)
      }
    }

    // may return a single action record, an array of action records, or null
    let newActionRecords = customize.actionRecordsFromHistoryRecord(projectName, historyRecord, inferredActionRecords)

    if (newActionRecords) {
      // wrap it as an array if they just returned one action record
      if (!Array.isArray(newActionRecords)) {
        newActionRecords = [newActionRecords]
      }
      for (let i=0;i<newActionRecords.length;i++) {
        const newActionRecord = newActionRecords[i]
        newActionRecord.type = "action" // allows getRewardedActions to assign rewards in one pass
        newActionRecord.timestamp = timestamp
        newActionRecord.timestampDate = timestampDate // for sorting. filtered out later
        // give each action a unique message id
        newActionRecord.message_id = i == 0 ? messageId : `${messageId}-${i}`;
        newActionRecord.history_id = historyId

        actionRecords.push(newActionRecord)
      }
    }
    
    // may return a single rewards record or null
    let newRewardsRecord = customize.rewardsRecordFromHistoryRecord(projectName, historyRecord)

    if (newRewardsRecord) {
      if (!naming.isObjectNotArray(newRewardsRecord.rewards)) {
        throw new Error(`rewards must be object type and not array ${JSON.stringify(newRewardsRecord)}`)
      } 
      
      newRewardsRecord.type = "rewards" // allows getRewardedActions to assign rewards in one pass
      // timestampDate is used for sorting
      newRewardsRecord.timestampDate = timestampDate
      rewardsRecords.push(newRewardsRecord)
    }
  }

  return assignRewardsToActions(actionRecords, rewardsRecords)
}

// in a single pass assign rewards to all action records
function assignRewardsToActions(actionRecords, rewardsRecords) {
  if (!rewardsRecords.length) {
    // for sparse rewards this should speed up processing considerably
    return actionRecords
  }
  
  // combine all the records together so we can process in a single pass
  const sortedRecords = actionRecords.concat(rewardsRecords).sort((a, b) => b.timestampDate - a.timestampDate)
  const actionRecordsByRewardKey = {}

  for (const record of sortedRecords) {
    // set up this action to listen for rewards
    if (record.type === "action") {
      let rewardKey = "reward" // default reward key
      if (record.reward_key) {
        rewardKey = record.reward_key
      }
      let listeners = actionRecordsByRewardKey[rewardKey]
      if (!listeners) {
        listeners = []
        actionRecordsByRewardKey[rewardKey] = listeners
      }
      // TODO robust configuration
      record.rewardWindowEndDate = new Date(record.timestampDate.getTime() + customize.config.rewardWindowInSeconds * 1000)
      listeners.push(record)
    } else if (record.type === "rewards") {
      // iterate through each reward key and find listening actions
      for (const [rewardKey, reward] of Object.entries(record.rewards)) {
        const listeners = actionRecordsByRewardKey[rewardKey]
        if (!listeners) {
          continue;
        }
        // loop backwards so that removing an expired listener doesn't break the array loop
        for (let i = listeners.length - 1; i >= 0; i--) {
          const listener = listeners[i]
          if (listener.rewardWindowEndDate < record.timestampDate) { // the listener is expired
            listeners.splice(i,1) // remove the element
          } else {
            listener.reward = (listener.reward || 0) + Number(reward) // Number allows booleans to be treated as 1 and 0
          }
        }
      }
    } else { 
      throw new Error(`type must be \"action\" or \"rewards\" ${JSON.stringify(record)}`)
    }
  }
  
  return actionRecords
}

function writeRewardedActions(projectName, shardId, rewardedActions) {
  console.log(`writing ${rewardedActions.length} rewarded action records for project ${projectName} shard ${shardId}`)
  const buffersByS3Key = {}
  for (let rewardedAction of rewardedActions) {
    const timestampDate = rewardedAction.timestampDate
    rewardedAction = finalizeRewardedAction(projectName, rewardedAction)
    const s3Key = naming.getRewardedActionS3Key(projectName, getModelForAction(projectName, rewardedAction.action), shardId, timestampDate)
    let buffers = buffersByS3Key[s3Key]
    if (!buffers) {
      buffers = []
      buffersByS3Key[s3Key] = buffers
    }
    buffers.push(Buffer.from(JSON.stringify(rewardedAction)+"\n"))
  }

  return Promise.all(Object.entries(buffersByS3Key).map(([s3Key, buffers]) => s3utils.compressAndWriteBuffers(s3Key, buffers)))
}

function finalizeRewardedAction(projectName, rewardedActionRecord) {
  let rewardedAction = _.pick(rewardedActionRecord, ["properties", "context", "action", "timestamp", "message_id", "history_id", "reward"])

  // an exception here will cause the entire history process task to fail
  rewardedAction = customize.modifyRewardedAction(projectName, rewardedAction)
  naming.assertValidRewardedAction(rewardedAction)
  
  return rewardedAction
}

// cached wrapper of naming.getModelForAction
const projectActionModelCache = {}
function getModelForAction(projectName, action) {
  // this is looked up for every rewarded action record during history procesing so needs to be fast
  let actionModelCache = projectActionModelCache[projectName]
  if (actionModelCache) {
    const model = actionModelCache[action]
    if (model) {
      return model
    }
  }
  
  const model = naming.getModelForAction(projectName, action)
  actionModelCache = {[action]: model}
  projectActionModelCache[projectName] = actionModelCache
  return model
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
