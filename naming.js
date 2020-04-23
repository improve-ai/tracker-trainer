'use strict';

const AWS = require('aws-sdk')
const mmh3 = require('murmurhash3js')
const _ = require('lodash')
const s3 = new AWS.S3()

const me = module.exports


// using power of 2 bit string shards makes future re-sharding easier than a modulo based approach
//const SHARD_COUNT = 2**Math.floor(Math.log2(process.env.HISTORY_SHARD_COUNT)) // round to floor power of 2

// intentionally not including this in the configuration and thus also not including it in the path names. Changing it would technically require deleting and regenerating all files from the firehose files since this would
// change which history files some events are assigned to.  In practice, changing this window shouldn't change cost/performance much unless a lot of delayed events were coming in from client SDKs.
// the reward windows and shard count are much more important for controlling cost/performance.  The maximum buffer window for firehose is 15 minutes (as of 2020/4) so this should catch most events.
// the history file is named based on its earliest event. Only allow events within the window in the file.
module.exports.getHistoryFileWindowMillis = () => {
  return 3600 * 1000 // one hour
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
    return me.getShardId(historyId, 1) // create either shard '0' or '1'
  } else {
    return sortedShards[index-1] // this fullShardId startsWith the previous shard, so use that
  }
}

// If a shard is in the process of being resharded, the longer child bitstring shard will be returned
module.exports.assignToHistoryS3Key = (sortedShards, projectName, event, uuid) => {
  if (!event.timestamp || !event.history_id) {
    throw new Error(`missing timestamp or history_id in ${JSON.stringify(event)}`)
  }
  // ensure that we're using UTC
  const [year, month, day] = new Date(event.timestamp).toISOString().slice(0,10).split('-')
  const shardId = me.assignToShard(sortedShards, event.history_id)
  
  // histories/data/projectName/shardId/yyyy/MM/dd/improve-events-shardId-yyyy-MM-dd-uuid.gz
  return `histories/data/${projectName}/${shardId}/${year}/${month}/${day}/improve-events-${shardId}-${year}-${month}-${day}-${uuid}.gz`
}

module.exports.getSortedChildShardsForS3Key = (parentS3Key) => {

  let shardId;
  
  if (me.isHistoryS3Key(parentS3Key)) {
    shardId = parentS3Key.split('/')[3]
  } else if (me.isRewardedActionS3Key(parentS3Key)) {
    shardId = parentS3Key.split('/')[6]
  } else {
     throw new Error(`parentS3Key ${parentS3Key} is not a history or rewarded action key`)
  }
  return [`${shardId}0`, `${shardId}1`]
}

module.exports.getChildS3Key = (parentS3Key, childShardId) => {
  if (me.isHistoryS3Key(parentS3Key)) {
    const split  = parentS3Key.split('/')
    split[3] = childShardId
    return split.join('/')
  } else if (me.isRewardedActionS3Key(parentS3Key)) {
    const split  = parentS3Key.split('/')
    split[6] = childShardId
    return split.join('/')
  } else {
     throw new Error(`parentS3Key ${parentS3Key} is not a history or rewarded action key`)
  }
}

module.exports.getVariantsS3Key = (projectName, modelName, firehoseS3Key) => {
  const dashSplitS3Key = firehoseS3Key.split('-')
  const [year, month, day, hour, minute, second] = dashSplitS3Key.slice(dashSplitS3Key.length-11, dashSplitS3Key.length - 5) // parse from the back to avoid unexpected dashes
  const firehoseUuid = firehoseS3Key.substring(firehoseS3Key.length-39, firehoseS3Key.length-3) // 36 uuid characters, 3 .gz characters

  // variants/data/projectName/modelName/yyyy/MM/dd/improve-variants-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `variants/data/${projectName}/${modelName}/${year}/${month}/${day}/improve-variants-${year}-${month}-${day}-${hour}-${minute}-${second}-${firehoseUuid}.gz`
}

module.exports.isHistoryS3Key = (s3Key) => {
  return s3Key.startsWith("histories/data")
}
    
module.exports.getHistoryS3KeyPrefix = (projectName) => {
  return `histories/data/${projectName}/`
}

module.exports.getHistoryShardS3KeyPrefix = (projectName, shardId) => {
  return `histories/data/${projectName}/${shardId}/`
}

module.exports.isIncomingHistoryS3Key = (s3Key) => {
  return s3Key.startsWith("histories/meta/incoming/")
}

module.exports.getIncomingHistoryS3Key = (s3Key) => {
  return `${me.getIncomingHistoryS3KeyPrefix()}${s3Key.substring("histories/data/".length)}.json`
}

module.exports.getIncomingHistoryS3KeyPrefix = () => {
  return "histories/meta/incoming/"
}

module.exports.getIncomingHistoryShardS3KeyPrefix = (projectName, shardId) => {
  return `${me.getIncomingHistoryS3KeyPrefix()}${projectName}/${shardId}/`
}

module.exports.getProjectNameFromHistoryS3Key = (historyS3Key) => {
  return historyS3Key.split('/')[2]
}

module.exports.isRewardedActionS3Key = (s3Key) => {
  return s3Key.startsWith("rewarded_actions/data")
}

module.exports.getRewardActionProjectS3KeyPrefix = (projectName) => {
  return `rewarded_actions/data/${projectName}/`
}

module.exports.getRewardedActionS3Key = (historyS3Key, modelName) => {
  const [histories, data, projectName, shardId, year, month, day, historyFileName] = historyS3Key.split('/')
  const joinedFileName = `improve-joined${historyFileName.substring('improve-events'.length)}`
  
  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return `rewarded_actions/data/${projectName}/${modelName}/${getTrainValidationPathPart(joinedFileName)}/${shardId}/${year}/${month}/${day}/${joinedFileName}`
}

module.exports.getRewardedActionS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/rewarded_actions/${projectName}/${modelName}`
}

module.exports.getRewardedActionTrainS3Uri = (projectName, modelName) => {
  return `${me.getJoinedS3Uri(projectName, modelName)}/${me.getTrainPathPart()}`
}

module.exports.getRewardedActionValidationS3Uri = (projectName, modelName) => {
  return `${me.getJoinedS3Uri(projectName, modelName)}/${me.getValidationPathPart()}`
}

/* 
 Implements the train/validation split based on the hash of the file name.  
 All file name transformations are idempotent to avoid duplicate records.
 projectName and modelName are not included in the hash to allow files to be copied between models
*/
function getTrainValidationPathPart(fileName) {
  let validationProportion = parseFloat(process.env.VALIDATION_PROPORTION)
  
  console.log(`mmh32 ${mmh3.x86.hash32(fileName)}`)
  if (mmh3.x86.hash32(fileName) / (2.0 ** 32) < validationProportion) {
    return module.exports.getValidationPathPart()
  } else {
    return module.exports.getTrainPathPart()
  }
}

module.exports.getTrainPathPart = () => {
  return `train/${100-(parseFloat(process.env.VALIDATION_PROPORTION)*100)}`
}

module.exports.getValidationPathPart = () => {
  return `validation/${parseFloat(process.env.VALIDATION_PROPORTION)*100}`
}

module.exports.getTransformedS3Uri = (projectName, model) => {
  return `s3://${process.env.RECORDS_BUCKET}/transformed/${projectName}/${model}/`
}

module.exports.getTransformedTrainS3Uri = (projectName, modelName) => {
  return `${me.getTransformedS3Uri(projectName, modelName)}/${me.getTrainPathPart()}`
}

module.exports.getTransformedValidationS3Uri = (projectName, modelName) => {
  return `${me.getTransformedS3Uri(projectName, modelName)}/${me.getValidationPathPart()}`
}

module.exports.getFeatureModelsS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/feature_models/${projectName}/${modelName}`
}

module.exports.getXGBoostModelsS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/xgboost_models/${projectName}/${modelName}`
}

// allow alphanumeric, underscore, dash, space, period
module.exports.isValidModelName = (modelName) => {
  return modelName.match(/^[\w\- .]+$/i)
}

module.exports.isValidProjectName = (projectName) => {
  return me.isValidModelName(projectName) // same rules
}

// from https://stackoverflow.com/questions/7445328/check-if-a-string-is-a-date-value
module.exports.isValidDate = (date) => {
  return (new Date(date) !== "Invalid Date") && !isNaN(new Date(date));
}

module.exports.getLambdaFunctionArn = (functionName, invokedFunctionArn) => {
  // arn:aws:lambda:us-west-2:117097735164:function:improve-v5-test-firehoseFileCreated
  const splitted = invokedFunctionArn.split('-')
  return `${splitted.slice(0,splitted.length-1).join('-')}-${functionName}`
}


function listAllIncomingHistoryShardS3Keys(projectName, shardId) {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getIncomingHistoryShardS3KeyPrefix(projectName, shardId)
  }

  return listAllKeys(params)
}

function listAllHistoryShardS3Keys(projectName, shardId) {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getHistoryShardS3KeyPrefix(projectName, shardId)
  }
  
  return listAllKeys(params)
}

function listAllRewardedActionShardS3Keys(projectName, shardId) {
  return listAllRewardedActionShardS3KeyPrefixes(projectName, shardId).then(prefixes => {
    return Promise.all(prefixes.map(prefix => {
      const params = {
        Bucket: process.env.RECORDS_BUCKET,
        Prefix: prefix
      }
      
      return listAllKeys(params)
    })).then(all => {
      return all.flat()
    })
  })
}

module.exports.listSortedHistoryShardsByProjectName = () => {
  const projectNames = Object.keys(customize.getProjectNamesToModelNamesMapping())
  return Promise.all(projectNames.map(projectName => {
    return listAllHistoryShards(projectName).then(shardIds => {
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

function listAllHistoryShards(projectName) {
  console.log(`listing shards for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getHistoryS3KeyPrefix(projectName)
  }
  
  return listAllSubPrefixes(params)
}

function listAllRewardedActionShardS3KeyPrefixes(projectName, shardId) {
  console.log(`listing models for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getRewardActionProjectS3KeyPrefix(projectName)
  }

  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return listAllPrefixes(params, 3)
}

function listAllPrefixes(params, depth=1) {
  console.log(`listing sub prefixes for ${JSON.stringify(params)}`)
  return listAllSubPrefixes(params).then(subPrefixes => {
    if (depth <= 1) {
      return subPrefixes.map(subPrefix => params.Prefix + subPrefix + params.Delimiter) 
    } else {
      return Promise.all(subPrefixes.map(subPrefix => listAllPrefixes(Object.assign(params, {Prefix: params.Prefix + subPrefix + params.Delimiter}), depth-1))).then(all => all.flat())
    }
  })
}

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
const listAllSubPrefixes = (params, out = []) => new Promise((resolve, reject) => {
  s3.listObjectsV2(params).promise()
    .then(({CommonPrefixes, IsTruncated, NextContinuationToken}) => {
      out.push(...CommonPrefixes.map(o => o.Prefix.split('/').slice(-2)[0])); // split and grab the second to last item from the Prefix
      !IsTruncated ? resolve(out) : resolve(listAllSubPrefixes(Object.assign(params, {ContinuationToken: NextContinuationToken}), out));
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