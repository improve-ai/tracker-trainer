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
//
// Options to ignore processing timers
// 
// force_processing: true
// force_continue_reshard: true
module.exports.dispatchRewardAssignmentWorkers = async (event, context) => {
  console.log(`processing lambda event ${JSON.stringify(event)}`)

  const projectNames = naming.allProjects()
  return Promise.all(projectNames.map(projectName => {
    // get all of the shard ids & load the shard timestamps
    return Promise.all([naming.listAllShards(projectName), shard.loadAndConsolidateShardLastProcessedDates(projectName)]).then(([shards, shardLastProcessedDates]) => {
      if (!shards.length) {
        console.log(`skipping project ${projectName} - no shards found`)
        return
      }
      const sortedShards = shards.sort() // sort() modifies shards
      // group the shards
      const [reshardingParents, reshardingChildren, nonResharding] = shard.groupShards(sortedShards)
      console.log(`project ${projectName} has ${shards.length-nonResharding.length}/${shards.length} shards currently resharding`)
      // check to see if any of the resharding parents didn't finish and sharding needs to be restarted
      // & check to see if any non-resharding shards have incoming history meta files and haven't been processed recently according to the timestamps
      return Promise.all([shard.dispatchReshardingIfNecessary(context, projectName, reshardingParents, shardLastProcessedDates, event.force_continue_reshard), 
                          dispatchAssignRewardsIfNecessary(context, projectName, nonResharding, shardLastProcessedDates, event.force_processing)])
    })
  }))
}

function dispatchAssignRewardsIfNecessary(lambdaContext, projectName, nonReshardingShards, lastProcessedDates, forceProcessing = false) {
  const nonReshardingShardsSet = new Set(nonReshardingShards)
  const now = new Date()
  const epoch = new Date(0)
  
  // list all incoming history shards
  return naming.listAllIncomingHistoryShards(projectName).then(incomingShards => {
    
    const incomingShardsWithDates = incomingShards.map(v => [v, (lastProcessedDates[v] || epoch)])
    
    // process the oldest ones first
    incomingShardsWithDates.sort((a, b) => a[1] - b[1])
    
    let remainingWorkers = Math.max(process.env.REWARD_ASSIGNMENT_WORKER_COUNT, 1)
    console.log(`assigning up to ${remainingWorkers} worker(s) for project ${projectName}`)
    
    return Promise.all(incomingShardsWithDates.map(([shardId, lastProcessed]) => {

      if (!forceProcessing && remainingWorkers <= 0) {
        return
      }

      // check if the incoming shard isn't currently being resharded and if it hasn't been processed too recently
      if (!forceProcessing && !nonReshardingShardsSet.has(shardId)) {
        console.log(`skipping project ${projectName} shard ${shardId} for history processing, currently resharding`)
        return 
      }
      
      if (!forceProcessing && (now - lastProcessed) < process.env.REWARD_ASSIGNMENT_REPROCESS_SHARD_WAIT_TIME_IN_SECONDS * 1000) {
        console.log(`skipping project ${projectName} shard ${shardId} for history processing, last processing ${lastProcessed.toISOString()} was too recent`)
        return
      }
      
      remainingWorkers--

      console.log(`invoking assignRewards for project ${projectName} shard ${shardId} last processed at ${lastProcessed.toISOString()}`)
  
      const params = {
        FunctionName: naming.getLambdaFunctionArn("assignRewards", lambdaContext.invokedFunctionArn),
        InvocationType: "Event",
        Payload: JSON.stringify({"project_name": projectName, "shard_id": shardId, "last_processed_timestamp_updated": true }) // mark that the last processed time is updated so that assignRewards doesn't have to also do it
      };
      
      // mark the last processed time as being updated here to have the smallest possible window where multiple redundant workers could be accidentally dispatched
      return Promise.all([shard.updateShardLastProcessed(projectName, shardId), lambda.invoke(params).promise()])
    }))
  })
}

module.exports.assignRewards = async function(event, context) {
  
  console.log(`processing event ${JSON.stringify(event)}`)

  const projectName = event.project_name
  const shardId = event.shard_id

  if (!projectName || !shardId) {
    throw new Error(`WARN: missing project_name or shard_id ${JSON.stringify(event)}`)
  }

  // since records for all models/namespaces for this project are lumped together, we need to use the max window sizes for processing
  const propensityWindowInSeconds = naming.getMaxPropensityWindowInSeconds(projectName)
  const rewardWindowInSeconds = naming.getMaxRewardWindowInSeconds(projectName)
  
  let updateLastProcessed = Promise.resolve(true)
  // this lambda should only ever be invoked from dispatchHistoryProcessingIfNecessary, but in case
  // some design changes in the future we want to make sure to mark the last processed time is guaranteed to be updated
  if (!event.last_processed_timestamp_updated) {
    updateLastProcessed = shard.updateShardLastProcessed(projectName, shardId)
  }

  // list the incoming keys and history keys for this shard
  return updateLastProcessed.then(() => Promise.all([naming.listAllHistoryShardS3KeysMetadata(projectName, shardId), naming.listAllIncomingHistoryShardS3Keys(projectName, shardId)]).then(([historyS3KeysMetadata, incomingHistoryS3Keys]) => {
    //
    // these date ranges are rather tricky. An incoming record could contain rewards for past decisions, propensities for future decisions, or actual decisions
    // incoming records are processed as one large window, with the minimum size being a single day, then at least one day one either end needs to be added for stale decisions records
    // then at least at an additional day needs to be added to each end for leading propensity records and trailing rewards rewards, for a minimum of 5 days processed in the
    // case of an incoming record that happened more than a day or two ago.
    //
    const incomingDateRange = getIncomingDateRange(incomingHistoryS3Keys) // the range of the actual incoming records
    console.log(`incoming records from ${incomingDateRange[0]} to ${incomingDateRange[1]}`)

    // calculate the range of only the stale decision records to re-process, using the incomingDateRange
    const staleDecisionsDateRange = getStaleDecisionsDateRange(incomingDateRange, propensityWindowInSeconds, rewardWindowInSeconds) 
    console.log(`stale decision records from ${staleDecisionsDateRange[0]} to ${staleDecisionsDateRange[1]}`)

    // calculate the whole window needed to process the stale decisions, including leading propensity records and trailing rewards records
    const historyWindowDateRange = getHistoryWindowDateRange(staleDecisionsDateRange, propensityWindowInSeconds, rewardWindowInSeconds)
    console.log(`required history window from ${historyWindowDateRange[0]} to ${historyWindowDateRange[1]}`)
    
    // get history S3 keys prior to the stale dates for joining propensity, and after the stale dates for joining rewards
    const historyWindowS3KeysMetadata = filterHistoryWindowS3KeysMetadata(projectName, historyS3KeysMetadata, historyWindowDateRange)

    // check size of the keys to be re-processed and reshard if necessary
    const totalSize = historyWindowS3KeysMetadata.reduce((acc, cur) => acc+cur.Size,0)
    console.log(`${totalSize} bytes of stale history data for project ${projectName} shard ${shardId}`)
    if (totalSize > (process.env.REWARD_ASSIGNMENT_WORKER_MAX_PAYLOAD_IN_MB * 1024 * 1024)) {
      console.log(`resharding project ${projectName} shard ${shardId} - stale history data is too large`)
      return shard.invokeReshardLambda(context, projectName, shardId)
    }
    
    return loadAndConsolidateHistoryRecords(historyWindowS3KeysMetadata.map(o => o.Key)).then(staleHistoryRecords => {

      // perform customize before we access the records
      staleHistoryRecords = customize.modifyHistoryRecords(projectName, staleHistoryRecords)

      // group history records by history id
      const historyRecordsByHistoryId = Object.fromEntries(Object.entries(_.groupBy(staleHistoryRecords, 'history_id')))

      // convert history records into rewarded decisions
      let rewardedDecisions = []
      
      for (const [historyId, historyRecords] of Object.entries(historyRecordsByHistoryId)) {
        rewardedDecisions = rewardedDecisions.concat(getRewardedDecisionsForHistoryRecords(projectName, historyId, historyRecords, staleDecisionsDateRange))
      }

      return writeRewardedDecisions(projectName, shardId, rewardedDecisions)      
    }).then(() => {
      // incoming has been processed, delete them.
      return s3utils.deleteAllKeys(incomingHistoryS3Keys)
    })
  }))
}

// returns a tuple of startDate (inclusive) and endDate (exclusive) for the given incoming history S3 Keys
function getIncomingDateRange(incomingHistoryS3Keys) {
  let startDate = new Date() // now
  let endDate = new Date(0) // unix epoch
  
  incomingHistoryS3Keys.forEach(k => {
    const keyStartDate = naming.getDateForIncomingHistoryS3Key(k) // midnight
    const keyEndDate = new Date(keyStartDate.getTime())
    keyEndDate.setUTCHours(24,0,0,0); // set to midnight of the following

    if (keyStartDate < startDate) {
      startDate = keyStartDate
    }
    if (keyEndDate > endDate) {
      endDate = keyEndDate
    }
  })
  
  return [startDate, endDate]
}

// returns a tuple of UTC midnight startDate (inclusive) and UTC midnight endDate (exclusive) of the date range of only the decision records that should be re-processed
function getStaleDecisionsDateRange(incomingDateRange, propensityWindowInSeconds, rewardWindowInSeconds) {
  const startDate = new Date(incomingDateRange[0].getTime())
  startDate.setSeconds(startDate.getSeconds()-rewardWindowInSeconds) // past decisions might need rewards records from incoming
  startDate.setUTCHours(0,0,0,0); // set to midnight of that day
  
  const endDate = new Date(incomingDateRange[1].getTime())
  endDate.setSeconds(endDate.getSeconds()+propensityWindowInSeconds) // future decisions might need propensity records from incoming
  endDate.setUTCHours(24,0,0,0); // set to midnight of the following
  
  return [startDate, endDate]
}

// returns a tuple of UTC midnight startDate (inclusive) and UTC midnight endDate (exclusive) of the entire range of history records that needs to be loaded to process the stale decision records, including past propensity records and future rewards records
function getHistoryWindowDateRange(staleDecisionsDateRange, propensityWindowInSeconds, rewardWindowInSeconds) {
  const startDate = new Date(staleDecisionsDateRange[0].getTime())
  startDate.setSeconds(startDate.getSeconds()-propensityWindowInSeconds) // decisions need previous propensity records
  startDate.setUTCHours(0,0,0,0); // set to midnight of that day
  
  const endDate = new Date(staleDecisionsDateRange[1].getTime())
  endDate.setSeconds(endDate.getSeconds()+rewardWindowInSeconds) // decisions need future rewards records
  endDate.setUTCHours(24,0,0,0); // set to midnight of the following
  
  return [startDate, endDate]
}

function filterHistoryWindowS3KeysMetadata(projectName, historyS3KeysMetadata, historyWindowDateRange) {
  const [startTime, endTime] = historyWindowDateRange
  
  console.log(`filtering history S3 keys for project ${projectName} from ${startTime} to ${endTime}}`)
  
  return historyS3KeysMetadata.filter(m => {
    const keyDate = naming.getDateForHistoryS3Key(m.Key) // UTC midnight of the key date
    return keyDate >= startTime && keyDate < endTime // the startTime and endTime will also land on midnight so be very careful of proper inclusive/exclusive semantics
  })
}

function loadAndConsolidateHistoryRecords(historyS3Keys) {
  const messageIds = new Set()
  let duplicates = 0
  let rewardsRecordCount = 0

  // group the keys by path
  return Promise.all(Object.entries(naming.groupHistoryS3KeysByDatePath(historyS3Keys)).map(([path, pathS3Keys]) => {
      // load all records from the s3 keys for this specific path
      return Promise.all(pathS3Keys.map(s3Key => s3utils.processCompressedJsonLines(process.env.RECORDS_BUCKET, s3Key, (record) => {
        // check for duplicate message ids.  This is most likely do to re-processing firehose files multiple times.
        const messageId = record.message_id
        if (!naming.isValidMessageId(messageId) || messageIds.has(messageId)) {
          duplicates++
          return null
        } else {
          messageIds.add(messageId)
          if (record.rewards) {
            rewardsRecordCount++
          }
          return record
        }
      }))).then(all => all.flat().filter(x => x)).then(records => {
        if (pathS3Keys.length == 1) {
          return records
        }

        console.log(`consolidating ${pathS3Keys.length} files at ${path} into 1 file`)
        const buffers = records.map(record => Buffer.from(JSON.stringify(record)+"\n"))
        return s3utils.compressAndWriteBuffers(naming.getConsolidatedHistoryS3Key(pathS3Keys[0]), buffers).then(() => {
          s3utils.deleteAllKeys(pathS3Keys)
        }).then(() => records)
      })
  })).then(all => all.flat()).then(records => {
    if (duplicates) {
      console.log(`ignoring ${duplicates} records with missing or duplicate message_id fields`)
    }
    console.log(`loaded ${records.length} records, ${rewardsRecordCount} with rewards`)

    return records
  })
}

// throw an Error on any parsing problems or customize bugs, causing the whole historyId to be skipped
// TODO parsing problems should be massaged before this point so that the only possible source of parsing bugs
// is customize calls.

// the history records will include the entire range of stale decision records plus the necessary earlier propensity records and later rewards records
// care must be taken to ensure that decision records outside the staleDecisionDateRange are not processed
// staleDecisionDateRange is a tuple of start date (inclusive) and end date (exclusive)
//
// some recent stale decisions may still have open reward windows.  In that case we process any rewards available up to the present moment.  As new rewards
// come in, those decisions will be made stale again and re-processed with the updated rewards.
function getRewardedDecisionsForHistoryRecords(projectName, historyId, historyRecords, staleDecisionsDateRange) {
  
  const [staleDecisionsStartDate, staleDecisionsEndDate] = staleDecisionsDateRange

  const propensityRecords = []
  let decisionRecords = []
  const rewardsRecords = []

  for (const historyRecord of historyRecords) {
    const messageId = historyRecord.message_id
    if (!naming.isValidMessageId(messageId)) {
      throw new Error(`invalid message_id for history record ${JSON.stringify(historyRecord)}`)
    }
    
    const timestampDate = naming.parseDate(historyRecord.timestamp)
    
    if (!timestampDate) {
      throw new Error(`invalid timestamp for history record ${JSON.stringify(historyRecord)}`)
    }

    // only process decision records that are in the stale decision date range
    if (timestampDate >= staleDecisionsStartDate && timestampDate < staleDecisionsEndDate) {
      // may return an array of decision records or null
      const newDecisionRecords = decisionRecordsFromHistoryRecord(projectName, historyRecord, historyId)
      if (newDecisionRecords) {
        decisionRecords = decisionRecords.concat(newDecisionRecords)
      }
    }

    // TODO also examine how the customize is working

    // may return a single propensity record or null
    const propensityRecord = propensityRecordFromHistoryRecord(historyRecord)
    if (propensityRecord) {
      propensityRecords.push(propensityRecord)
    }

    // may return a single rewards record or null
    const rewardsRecord = rewardsRecordFromHistoryRecord(historyRecord)
    if (rewardsRecord) {
      rewardsRecords.push(rewardsRecord)
    }
  }

  return assignPropensitiesAndRewardsToDecisions(propensityRecords, decisionRecords, rewardsRecords)
}

function decisionRecordsFromHistoryRecord(projectName, historyRecord, historyId) {
  let inferredDecisionRecords; // may remain null or be an array of decisionRecords
  
  // the history record may be of type "decision", in which case it itself is an decision record
  if (historyRecord.type === "decision") {
    inferredDecisionRecords = [historyRecord]
  }
  
  // the history record may have attached "decisions"
  if (historyRecord.decisions) {
    if (!Array.isArray(historyRecord.decisions)) {
      throw new Error(`attached decisions must be array type ${JSON.stringify(historyRecord)}`) // TODO
    } 
    
    if (!inferredDecisionRecords) {
      inferredDecisionRecords = historyRecord.decisions
    } else {
      inferredDecisionRecords = inferredDecisionRecords.concat(historyRecord.decisions)
    }
  }

  // may return a single decision record, an array of decision records, or null
  let decisionRecords = customize.decisionRecordsFromHistoryRecord(projectName, historyRecord, inferredDecisionRecords)

  if (decisionRecords) {
    // wrap it as an array if they just returned one decision record
    if (!Array.isArray(decisionRecords)) {
      decisionRecords = [decisionRecords]
    }
    for (let i=0;i<decisionRecords.length;i++) {
      const newDecisionRecord = decisionRecords[i]
      newDecisionRecord.type = "decision" // allows getRewardedDecisions to assign rewards in one pass
      newDecisionRecord.timestamp = historyRecord.timestamp
      // give each decision a unique message id
      newDecisionRecord.message_id = i == 0 ? historyRecord.messageId : `${historyRecord.messageId}-${i}`;
      newDecisionRecord.history_id = historyId
    }
  }
  
  return decisionRecords
}

// return null or a single propensity record that includes propensity, timestamp and type === propensity
function propensityRecordFromHistoryRecord(historyRecord) {
  let propensityRecord = null
  
  if (historyRecord.propensity && _.isFinite(historyRecord.propensity)) {
    propensityRecord = _.pick("propensity", "variant", "timestamp") // copy so that each record is a unique object
    propensityRecord.type = "propensity" // allows getRewardedDecisions to assign rewards in one pass
  }

  return propensityRecord
}

// return null or a single rewards record that includes rewards, timestamp and type === rewards
function rewardsRecordFromHistoryRecord(historyRecord) {
  let rewardsRecord = null

  if (historyRecord.rewards && naming.isObjectNotArray(historyRecord.rewards)) {
    rewardsRecord = _.pick("rewards", "timestamp") // copy so that each record is a unique object
    rewardsRecord.type = "rewards" // allows getRewardedDecisions to assign rewards in one pass
  }
  
  return rewardsRecord
}

// in a single pass assign propensities and rewards to all decision records
function assignPropensitiesAndRewardsToDecisions(propensityRecords, decisionRecords, rewardsRecords) {
  if (!rewardsRecords.length && !propensityRecords.length) {
    // nothing to join
    return decisionRecords
  }

  // -1 millis to propensity, +1 millis to rewards to ensure proper sorting when they come from the same event
  propensityRecords.forEach(e => e.timestampTime = new Date(e.timestamp).getTime()-1)
  decisionRecords.forEach(e => e.timestampTime = new Date(e.timestamp).getTime())
  propensityRecords.forEach(e => e.timestampTime = new Date(e.timestamp).getTime()+1)

  // TODO propensities
  
  // combine all the records together so we can process in a single pass
  const sortedRecords = decisionRecords.concat(rewardsRecords).sort((a, b) => a.timestampTime - b.timestampTime)
  const decisionRecordsByRewardKey = {}
  
  for (const record of sortedRecords) {
    // set up this decision to listen for rewards
    if (record.type === "decision") {
      let rewardKey = "reward" // default reward key
      if (record.reward_key) {
        rewardKey = record.reward_key
      }
      let listeners = decisionRecordsByRewardKey[rewardKey]
      if (!listeners) {
        listeners = []
        decisionRecordsByRewardKey[rewardKey] = listeners
      }
      // TODO robust configuration
      record.rewardWindowEndTime = record.timestampTime + customize.config.rewardWindowInSeconds * 1000
      listeners.push(record)
    } else if (record.type === "rewards") {
      // iterate through each reward key and find listening decisions
      for (const [rewardKey, reward] of Object.entries(record.rewards)) {
        const listeners = decisionRecordsByRewardKey[rewardKey]
        if (!listeners) {
          continue;
        }
        // loop backwards so that removing an expired listener doesn't break the array loop
        for (let i = listeners.length - 1; i >= 0; i--) {
          const listener = listeners[i]
          if (listener.rewardWindowEndTime < record.timestampTime) { // the listener is expired
            listeners.splice(i,1) // remove the element
          } else {
            listener.reward = (listener.reward || 0) + Number(reward) // Number allows booleans to be treated as 1 and 0
          }
        }
      }
    } else { 
      throw new Error(`type must be \"decision\" or \"rewards\" ${JSON.stringify(record)}`)
    }
  }
  
  return decisionRecords
}

function writeRewardedDecisions(projectName, shardId, rewardedDecisions) {
  const buffersByS3Key = {}
  let rewardedRecordCount = 0
  let totalRewards = 0
  let maxReward = 0

  for (let rewardedDecision of rewardedDecisions) {
    rewardedDecision = finalizeRewardedDecision(projectName, rewardedDecision)

    const reward = rewardedDecision.reward
    if (reward) {
      rewardedRecordCount++
      totalRewards += reward
      maxReward = Math.max(reward, maxReward)
    }

    const s3Key = naming.getRewardedDecisionS3Key(projectName, getModelForNamespace(projectName, rewardedDecision.namespace), shardId, new Date(rewardedDecision.timestamp))
    let buffers = buffersByS3Key[s3Key]
    if (!buffers) {
      buffers = []
      buffersByS3Key[s3Key] = buffers
    }
    buffers.push(Buffer.from(JSON.stringify(rewardedDecision)+"\n"))
  }

  console.log(`writing ${rewardedDecisions.length} rewarded decision records for project ${projectName} shard ${shardId}`)
  if (rewardedDecisions.length) {
    console.log(`(max reward ${maxReward}, mean reward ${totalRewards/rewardedDecisions.length}, non-zero rewards ${rewardedRecordCount})`)
  }

  return Promise.all(Object.entries(buffersByS3Key).map(([s3Key, buffers]) => s3utils.compressAndWriteBuffers(s3Key, buffers)))
}

function finalizeRewardedDecision(projectName, rewardedDecisionRecord) {
  let rewardedDecision = _.pick(rewardedDecisionRecord, ["variant", "context", "namespace", "timestamp", "message_id", "history_id", "reward", "propensity"])

  // an exception here will cause the entire history process task to fail
  rewardedDecision = customize.modifyRewardedDecision(projectName, rewardedDecision)
  naming.assertValidRewardedDecision(rewardedDecision)
  
  return rewardedDecision
}

// cached wrapper of naming.getModelForDecision
const projectNamespaceModelCache = {}
function getModelForNamespace(projectName, namespace) {
  // this is looked up for every rewarded namespace record during history procesing so needs to be fast
  let namespaceModelCache = projectNamespaceModelCache[projectName]
  if (namespaceModelCache) {
    const model = namespaceModelCache[namespace]
    if (model) {
      return model
    }
  }
  
  const model = naming.getModelForNamespace(projectName, namespace)
  namespaceModelCache = {[namespace]: model}
  projectNamespaceModelCache[projectName] = namespaceModelCache
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
