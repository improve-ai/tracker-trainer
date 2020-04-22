const dateFormat = require('date-format')
const mmh3 = require('murmurhash3js')
const uuidv4 = require('uuid/v4');
const _ = require('lodash')

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
    
module.exports.getHistoryS3Key = (projectName, shardId, earliestEventAt) => {
  
  // FIX double check for UTC time on earliest Event
  
  if (isNaN(earliestEventAt)) {
    throw `invalid earliestEventAt ${JSON.stringify(earliestEventAt)}`
  }
  
  const pathDatePart = dateFormat.asString("yyyy/MM/dd", earliestEventAt)
  const filenameDatePart = dateFormat.asString("yyyy-MM-dd", earliestEventAt)

  // histories/data/projectName/shardId/yyyy/MM/dd/improve-events-shardId-yyyy-MM-dd-uuid.gz
  return `histories/data/${projectName}/${shardId}/${pathDatePart}/improve-events-${shardId}-${filenameDatePart}-${uuidv4()}.gz`
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
  return `${me.getIncomingHistoryS3KeyPrefix()}${s3Key.substring("histories/data/".length)}`
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

module.exports.getRewardedActionS3Key = (historyS3Key, modelName) => {
  const [histories, data, projectName, shardId, year, month, day, hour, historyFileName] = historyS3Key.split('/')
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