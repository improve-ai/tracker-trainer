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

      // check to see if any of the resharding parents didn't finish and sharding needs to be restarted
      // & check to see if any non-resharding shards have incoming history meta files and haven't been processed recently according to the timestamps
      return Promise.all([shard.dispatchReshardingIfNecessary(content, projectName, reshardingParents, shardLastProcessedDates), 
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
  // TODO history keys should be history listing results with size information
  return updateLastProcessed.then(() => Promise.all([naming.listAllHistoryShardS3Keys(projectName, shardId), naming.listAllIncomingHistoryShardS3Keys(projectName, shardId)]).then(([historyS3Keys, incomingHistoryS3Keys]) => {
    const staleS3Keys = filterStaleHistoryS3Keys(historyS3Keys, incomingHistoryS3Keys)
    
    // check size of the keys to be re-processed and reshard if necessary
    if (false) {
      console.log(`resharding project ${projectName} shard ${shardId} - stale history data is too large ${bytes}`)
      return shard.invokeReshardLambda(context, projectName, shardId)
    }
    
    return loadAndConsolidateHistoryRecords(staleS3Keys).then(staleHistoryRecords => {
      // group history records by history id
      const historyRecordsByHistoryId = Object.fromEntries(Object.entries(_.groupBy(staleHistoryRecords, 'history_id')))

      // convert history records into rewarded actions
      let rewardedActions = []
      
      for (const [historyId, historyRecords] of Object.entries(historyRecordsByHistoryId)) {
        try {
          rewardedActions = rewardedActions.concat(getRewardedActionsForHistoryRecords(projectName, historyId, historyRecords))
        } catch (err) {
          console.log(`WARN: skipping history id ${historyId} history records ${historyRecords}`, err)
        }
      }

      return writeRewardedActions(rewardedActions)      
    }).then(() => {
      // incoming has been processed, delete them.
      return s3utils.deleteAllKeys(incomingHistoryS3Keys)
    })
  }))
}


function filterStaleHistoryS3Keys(historyS3Keys, incomingHistoryS3Keys) {
  return historyS3Keys
}

// throw an Error on any parsing problems or customize bugs, causing the whole historyId to be skipped
function getRewardedActionsForHistoryRecords(projectName, historyId, historyRecords) {
  
  historyRecords = customize.modifyHistoryRecords(projectName, historyId, historyRecords)
  
  const actionRecords = []
  const rewardsRecords = []
  for (const historyRecord of historyRecords) {
    // make sure the history ids weren't modified in customize
    if (historyId !== historyRecord.history_id) {
      throw new Error(`historyId ${historyId} does not match record ${JSON.stringify(historyRecord)}`)
    }
    
    // grab all the values that we don't want to change before during further calls to customize
    const timestamp = historyRecord.timestamp
    const timestampDate = new Date(timestamp)
    if (!timestamp || isNaN(timestampDate.getTime())) {
      throw new Error(`invalid timestamp for history record ${JSON.stringify(historyRecord)}`)
    }
    const messageId = historyRecord.message_id
    if (!messageId || !naming.isString(messageId)) {
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

    // may return an array of action records or null
    let newActionRecords = customize.actionRecordsFromHistoryRecord(projectName, historyRecord, inferredActionRecords)

    if (newActionRecords) {
      for (let i=0;i<newActionRecords.length;i++) {
        const newActionRecord = newActionRecords[i]
        newActionRecord.timestamp = timestamp
        newActionRecord.timestampDate = timestampDate // for sorting. filtered out later
        // give each one a unique message id
        newActionRecord.message_id = i == 0 ? messageId : `${messageId}-${i}`;
        actionRecords.push(newActionRecord)
      }
    }
    
    // may return a single rewards record or null
    let newRewardsRecord = customize.rewardsRecordFromHistoryRecord(projectName, historyRecord)

    if (newRewardsRecord) {
      if (!naming.isObjectNotArray(newRewardsRecord.reward)) {
        throw new Error(`rewards must be object type and not array ${JSON.stringify(newRewardsRecord)}`)
      } 
      
      // timestampDate is used for sorting
      newRewardsRecord.timestampDate = timestampDate
      rewardsRecords.push(newRewardsRecord)
    }
  }

  const sortedRewardsRecords = rewardsRecords.sort((a, b) => b.timestampDate - a.timestampDate)

  return actionRecords.map(actionRecord => getRewardedAction(projectName, historyId, actionRecord, sortedRewardsRecords))
}

// sets timestamp plus adds timestampDate to the record
function assignTimestampToRecord(timestamp, record) {
  record.timestamp = timestamp
  // used for sorting and reward assignment. should be removed later
  record.timestampDate = new Date(timestamp)
}

function getRewardedAction(projectName, historyId, actionRecord, sortedRewardsRecords) {
  let rewardedAction = _.pick(actionRecord, ["properties", "context", "action", "timestamp", "message_id"])
  rewardedAction.history_id = historyId
  
  rewardedAction.reward = getRewardForActionRecord(actionRecord, sortedRewardsRecords)

  // don't send customize bad data and don't allow bad data from customize
  naming.assertValidRewardedAction(rewardedAction)
  rewardedAction = customize.modifyRewardedAction(projectName, rewardedAction)
  naming.assertValidRewardedAction(rewardedAction)
  
  return rewardedAction
}

function getRewardForActionRecord(actionRecord, sortedRewardsRecords) {
  let reward = 0
  // find start position
  _.sortedIndexBy(objects, { 'timestampDate': 4 }, 'timestampDate');
  
  return reward
}

// This function may also return a promise for performing asynchronous processing
module.exports.assignRewardedActionsToModelsFromHistoryEvents = (projectName, sortedHistoryEvents) => {
    const modelsToJoinedEvents = {}
    const rewardKeysToEvents = {}
    
    for (const record of sortedHistoryEvents) {
        if (record.record_type) {
            if (record.record_type === "using" && record.reward_key) {
                record.reward = 0
                if (!modelsToJoinedEvents[record.model]) {
                    modelsToJoinedEvents[record.model] = []
                }
                modelsToJoinedEvents[record.model].push(record)
                rewardKeysToEvents[record.reward_key] = record
            } else if (record.record_type === "rewards" && record.rewards) {
                for (const [rewardKey, reward] of Object.entries(record.rewards)) {
                    if (rewardKey in rewardKeysToEvents) {
                        rewardKeysToEvents[rewardKey].reward = rewardKeysToEvents[rewardKey].reward + reward
                    }
                }
            }
        }
    }
    
    return modelsToJoinedEvents;
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
