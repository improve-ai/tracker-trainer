'use strict';

const AWS = require('aws-sdk')
const lambda = new AWS.Lambda()
const s3 = new AWS.S3()
const mmh3 = require('murmurhash3js')
const _ = require('lodash')
const naming = require("./naming.js")
const history = require("./history.js")
const s3utils = require("./s3utils.js")

const me = module.exports

// invoke reshardFile lambda for every history and rewarded decision S3 key associated with this shard. reshardFile has a maximum reserved concurrency
// so some of the tasks may be queued
//
// It is possible that firehose can be simultaneously writing new files with the old parent shards that will be missed by this process.
// On a modest sized deployment this should not happen during nornal production since enough time should elapse between firehose file
// creation for dispatchHistoryProcessingWorkers to take place.  This is very common during massive ingest processes.
// In that case we have to wait 6 hours and 15 minutes before attempting resharding again.
module.exports.splitShard = async (event, context) => {
  console.log(`processing event ${JSON.stringify(event)}`)
  
  const projectName = event.project_name
  const shardId = event.shard_id
  
  if (!projectName || !shardId) {
    throw new Error(`WARN: missing project_name or shard_id ${JSON.stringify(event)}`)
  }

  // update the last processing time. the workers will get 6 hours (max lambda queue time) plus 15 minutes (max worker time) to finish resharding until it is retried
  return me.updateShardLastProcessed(projectName, shardId).then(() => {
    // fetch all of the history and rewarded decision keys for this shard
    return Promise.all([naming.listAllHistoryShardS3Keys(projectName, shardId), naming.listAllIncomingHistoryShardS3Keys(projectName, shardId), naming.listAllRewardedDecisionShardS3Keys(projectName, shardId)]).then(all => all.flat()).then(s3Keys => {
      if (s3Keys.length == 0) {
        // resharding is complete. continue history processing.
        return Promise.all(getSortedChildShards(shardId).map(childShardId => {
          console.log(`invoking assignRewards for project ${projectName} shard ${childShardId}`)
      
          const params = {
            FunctionName: naming.getLambdaFunctionArn("assignRewards", context.invokedFunctionArn),
            InvocationType: "Event",
            Payload: JSON.stringify({"project_name": projectName, "shard_id": childShardId }) // mark that the last processed time is updated so that assignRewards doesn't have to also do it
          };
          return lambda.invoke(params).promise()
        }))
      }
      
      // handle resharding
      return Promise.all(s3Keys.map(s3Key => {
        
        if (naming.isIncomingHistoryS3Key(s3Key)) {
          // handle incoming history s3keys right here because it is quick, low memory, and there shouldn't be many
          return reshardIncomingHistoryS3Key(s3Key)
        }
        
        const lambdaArn = naming.getLambdaFunctionArn("splitFile", context.invokedFunctionArn)
        console.log(`invoking reshardFile for ${s3Key}`)
      
        const params = {
          FunctionName: lambdaArn,
          InvocationType: "Event",
          Payload: JSON.stringify({"s3_key": s3Key})
        };
        
        return lambda.invoke(params).promise()
      })).then(() => {
        // wait until there is 30 seconds of execution time remaining then recurse to pick up any new history files that were written when
        // reshard was invoked.  This often occurs when files are bulk copied into the firehose bucket.  During normal operation
        // the recurse will just make sure that assignRewards is called to continue history processing for the new child shards.
        // In the case of slow reshard processing, it is possible that the recursive execution could place redundant reshard tasks on the
        // lambda queue.  The queue has a capped reservedConcurrency, so it is unlikely that the exact same file will be processed twice
        // simultaneously (which is ultimately okay since a reshard is idempotent), and the second reshard for that file should complete quickly
        // since the file will not be found.
        const waitTime = Math.max(0, context.getRemainingTimeInMillis()-30000)
        console.log(`waiting ${waitTime} millis before recursing`)
        return new Promise(resolve => setTimeout(resolve, waitTime)).then(() => {
          return me.invokeReshardLambda(context, projectName, shardId)
        })
      })
    })
  })
}

// reshard either history or rewarded_decision files
// for rewarded_decision files there is a small window where there may be some duplicates, but this is okay for model training
// since it is fairly robust to variations in weighting.
module.exports.splitFile = async function(event, context) {
  console.log(`processing event ${JSON.stringify(event)}`)

  const s3Key = event.s3_key
  
  if (!s3Key) {
    throw new Error(`WARN: missing s3_key ${JSON.stringify(event)}`)
  }
  
  // just contains two entries. one with a zero appended, the other with a one
  const sortedChildShards = me.getSortedChildShardsForS3Key(s3Key)
  
  const buffersByS3Key = {}
  // load the data from the key to split
  return s3utils.processCompressedJsonLines(process.env.RECORDS_BUCKET, s3Key, record => {
    if (!record.history_id || !record.timestamp) {
      console.log(`WARN: skipping, missing history_id or timestamp ${JSON.stringify(record)}`)
    }
    
    const shardId = me.assignToShard(sortedChildShards, record.history_id)
    // pick which child key this record goes to
    const childS3Key = naming.replaceShardIdForS3Key(s3Key, shardId, new Date(record.timestamp))

    let buffers = buffersByS3Key[childS3Key]
    if (!buffers) {
      buffers = []
      buffersByS3Key[childS3Key] = buffers
    }
    buffers.push(Buffer.from(JSON.stringify(record)+"\n"))
  }).then(() => {
    // write out the records
    return Promise.all(Object.entries(buffersByS3Key).map(([s3Key, buffers]) => {
      return s3utils.compressAndWriteBuffers(s3Key, buffers)
    }))
  }).then(() => {
    // delete the parent file
    return s3utils.deleteKey(s3Key)
  })
}

function reshardIncomingHistoryS3Key(s3Key) {
  console.log(`resharding incoming history s3Key ${s3Key}`)
  if (!naming.isIncomingHistoryS3Key(s3Key)) {
    throw new Error("s3Key ${s3Key} must be an incoming history s3 key")
  }
  
  // get the original history s3key that this is marking
  const historyS3Key = naming.getHistoryS3KeyForIncomingHistoryS3Key(s3Key)
  
  // split the original history key into two
  return Promise.all(me.getSortedChildShardsForS3Key(historyS3Key).map(childShard => {
      // mark each child history key as incoming
      return history.markHistoryS3KeyAsIncoming(naming.replaceShardIdForHistoryS3Key(historyS3Key, childShard))
  })).then(() => {
    // delete the parent file
    return s3utils.deleteKey(s3Key)
  })
}

// called from history.dispatchProcessingWorkers
// reshard workers have limited reservedConcurrency so may queue for up to 6 hours (plus max 15 minutes of execution) until they should be retried
// https://docs.aws.amazon.com/lambda/latest/dg/invocation-async.html
module.exports.dispatchReshardingIfNecessary = (lambdaContext, projectName, reshardingParents, lastProcessedDates, forceContinueReshard = false) => {
  const now = new Date()
  const unix_epoch = new Date(0)
  return Promise.all(reshardingParents.map(shardId => {
    const lastProcessed = lastProcessedDates[shardId] || unix_epoch

    if (forceContinueReshard || ((now - lastProcessed) > 6 * 60 * 60 * 1000 + 15 * 60 * 1000)) {
      if (!forceContinueReshard) {
        console.log(`WARN: project ${projectName} shard ${shardId} initialized resharding over 6 hours 15 minutes ago at ${lastProcessed.toISOString()}. Shard worker reservedConcurrency may be too low.`)   
      }
      // all of the files have been written and meta incoming files have been touched, dispatch the workers to process
      return me.invokeReshardLambda(lambdaContext, projectName, shardId)
    }
  }))
}

module.exports.invokeReshardLambda = (lambdaContext, projectName, shardId) => {
  console.log(`invoking reshard for project ${projectName} shard ${shardId}`)

  const params = {
    FunctionName: naming.getLambdaFunctionArn("splitShard", lambdaContext.invokedFunctionArn),
    InvocationType: "Event",
    Payload: JSON.stringify({"project_name": projectName, "shard_id": shardId})
  }
  
  return lambda.invoke(params).promise()
}


module.exports.getShardId = (historyId, bitCount=32) => {
  // generate a 32 bit bit string and truncate so that we can easily reshard
  // a modulo based approach would require a complete remapping every time the number of shards changes
  return mmh3.x86.hash32(historyId).toString(2).padStart(32, '0').substring(0, bitCount)
}

// If a shard is in the process of being resharded, the longer child bitstring shard will be returned
module.exports.assignToShard = (sortedShards, historyId) => {
  const fullShardId = me.getShardId(historyId)
  
  // do a binary search for the full shard id
  const index = _.sortedIndex(sortedShards, fullShardId)
  // the previous index should be a parent of this id, if not, create either shard '0' or '1'
  if (index <= 0 || !fullShardId.startsWith(sortedShards[index-1])) {
    return me.getShardId(historyId, 10) // create either shard '0' or '1'
  } else {
    return sortedShards[index-1] // this fullShardId startsWith the previous shard, so use that
  }
}

// If a shard is in the process of being resharded, the longer child bitstring shard will be returned
module.exports.assignToHistoryS3Key = (sortedShards, projectName, historyId, utcTimestampString, uuid) => {
  const shardId = me.assignToShard(sortedShards, historyId)
  
  return naming.getHistoryS3Key(projectName, shardId, utcTimestampString, uuid)
}

module.exports.getSortedChildShardsForS3Key = (parentS3Key) => {
  return getSortedChildShards(naming.getShardIdForS3Key(parentS3Key))
}

function getSortedChildShards(shardId) {
  return [`${shardId}0`, `${shardId}1`]
}

module.exports.groupShards = (sortedShards) => {
  const reshardingParents = []
  const nonReshardingParents = []
  
  // one less than the length since we're peaking ahead
  for (let i=0;i<sortedShards.length-1;i++) {
    let current = sortedShards[i]
    let next = sortedShards[i+1]
    if (next <= current) { // sanity check
      throw new Error(`shards not sorted or contain duplicates ${JSON.stringify(sortedShards)}`)
    }
    if (next.startsWith(current)) {
      reshardingParents.push(current)        
    } else {
      nonReshardingParents.push(current)
    }
  }
  // push the last element
  nonReshardingParents.push(sortedShards[sortedShards.length-1])
  
  // there can be multiple children, so handle them in this phase
  const reshardingChildren = []
  const nonResharding = []
  for (const shard of nonReshardingParents) {
    // if it starts with any parent its a child
    if (reshardingParents.some(reshardingParent => shard.startsWith(reshardingParent))) {
      reshardingChildren.push(shard)    
    } else {
      nonResharding.push(shard)
    }
  }
  
  // sanity check
  if (reshardingParents.length+reshardingChildren.length+nonResharding.length != sortedShards.length) {
    throw new Error(`assert fail: input/output length mistmatch: original ${JSON.stringify(sortedShards)} parents ${JSON.stringify(reshardingParents)} children ${JSON.stringify(reshardingChildren)} non ${JSON.stringify(nonResharding)}`)
  }
  
  return [reshardingParents, reshardingChildren, nonResharding]
}

module.exports.updateShardLastProcessed = (projectName, shardId) => {
  console.log(`marking shard last processed for project ${projectName} shard ${shardId}`)
  // serialize a map into a UUID based file name. This will be consolidated later into the larger list of shards
  const lastProcessed = {}
  lastProcessed[shardId] = new Date().toISOString()
  const params = {
    Body: JSON.stringify(lastProcessed),
    Bucket: process.env.RECORDS_BUCKET,
    Key: naming.getUniqueShardTimestampsS3Key(projectName)
  }

  return s3.putObject(params).promise()
}

module.exports.loadAndConsolidateShardLastProcessedDates = (projectName) => {
  if (!projectName) {
    throw new Error("projectName required")
  }
  console.log(`loading shard timestamps for ${projectName}`)
  
  // get all of the shard timestamp keys for tracking when the shard was last processed or resharded
  return naming.listAllShardTimestampsS3Keys(projectName).then(s3Keys => {
    return Promise.all(s3Keys.map(s3Key => {
      const params = {
        Bucket: process.env.RECORDS_BUCKET,
        Key: s3Key
      }
      return s3.getObject(params).promise().then(data => {
        return JSON.parse(data.Body.toString('utf-8'))
      })
    })).then(shardTimestampFiles => {
      // consolidate all dates into a single object
      const consolidatedLastProcessedDates = {}
      for (const shardTimestamps of shardTimestampFiles) {
        for (const [shardId, lastProcessedTimestamp] of Object.entries(shardTimestamps)) {
          const lastProcessedDate = new Date(lastProcessedTimestamp)
          if (isNaN(lastProcessedDate.getTime())) {
            console.log(`WARN: skipping invalid timestamp ${JSON.stringify(lastProcessedTimestamp)}`)
            continue
          }
          if (!consolidatedLastProcessedDates.hasOwnProperty(shardId) || lastProcessedDate > consolidatedLastProcessedDates[shardId]) {
            consolidatedLastProcessedDates[shardId] = lastProcessedDate
          }
        }
      }

      // Don't write/delete if there is just one key
      // if a huge number of firehose files are being ingested there may be a lot of calls to this method without any new timestamps
      // being marked.  It just seems like a good idea to avoid the extra puts and deletes in case some S3 error caused a put
      // to get lost.
      if (s3Keys.length == 1) {
        console.log(`skipping shard timestamp consolidation for project ${projectName}, only one timestamp file`)
        return consolidatedLastProcessedDates
      }
      
      const uniqueShardTimestampsS3Key = naming.getUniqueShardTimestampsS3Key(projectName)
      console.log(`saving consolidated shard timestamps to ${uniqueShardTimestampsS3Key}`)
      // create a new unique file that consolidates all current timestamps
      // by saving the consolidated state as a new file with a uuid and then deleting any previous files we can be fairly confident
      // that we've rolled up all previously updated state.  In the event that two updates were to happen simultaneously, they would
      // be rolled up in the future.
      let params = {
        Body: JSON.stringify(Object.fromEntries(Object.entries(consolidatedLastProcessedDates).map(([k,d]) => [k,d.toISOString()]))), // convert to ISO timestamp for serialization
        Bucket: process.env.RECORDS_BUCKET,
        Key: uniqueShardTimestampsS3Key
      }
      return s3.putObject(params).promise().then(() => {
        // clean up all s3 keys that were consolidated into this new file
        return s3utils.deleteAllKeys(s3Keys).then(() => consolidatedLastProcessedDates)
      })
    })
  })
}