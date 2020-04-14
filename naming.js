const dateFormat = require('date-format')
const mmh3 = require('murmurhash3js')

const me = module.exports

/*
  All naming in this system is designed to allow idempotent re-processing of files at any point in the processing pipeline without creating duplicate data downstream.  For this reason
  pipelines are seperated by SHARD_COUNT.  If the SHARD_COUNT is changed then a new processing pipeline will be created to ensure that s3 keys stay consistent and idempotent.
  
  History files are further seperated by the history file window
  
  joined/transformed files are further seperated by the VALIDATION_PROPORTION
  
  As long as these variables stay constant, re-processing of firehose files should be idempotent
*/

// using power of 2 bit string shards makes future re-sharding easier than a modulo based approach
const SHARD_COUNT = 2**Math.floor(Math.log2(process.env.HISTORY_SHARD_COUNT)) // round to floor power of 2

// intentionally not including this in the configuration and thus also not including it in the path names. Changing it would technically require deleting and regenerating all files from the firehose files since this would
// change which history files some events are assigned to.  In practice, changing this window shouldn't change cost/performance much unless a lot of delayed events were coming in from client SDKs.
// the reward windows and shard count are much more important for controlling cost/performance.  The maximum buffer window for firehose is 15 minutes (as of 2020/4) so this should catch most events.
// the history file is named based on its earliest event. Only allow events within the window in the file.
module.exports.getHistoryFileWindowMillis = () => {
  return 3600 * 1000 // one hour
}

module.exports.getShardId = (userId) => {
  const bitCount = Math.floor(Math.log2(SHARD_COUNT))
  // generate a 32 bit bit string and truncate so that we're possibly forward compatible with different subspace queries and re-sharding logic
  // a modulo based approach would require a complete remapping every time the number of shards changes
  // only depends on the userId to allow files to moved between projects and models with the same shard count and still avoid duplicates if the firehose file is reprocessed
  return mmh3.x86.hash32(userId).toString(2).padStart(32, '0').substring(0, bitCount)
}

module.exports.getVariantsS3Key = (projectName, modelName, firehoseS3Key) => {
  const dashSplitS3Key = firehoseS3Key.split('-')
  const [year, month, day, hour, minute, second] = dashSplitS3Key.slice(dashSplitS3Key.length-11, dashSplitS3Key.length - 5)
  const firehoseUuid = firehoseS3Key.substring(firehoseS3Key.length-39, firehoseS3Key.length-3) // 36 uuid characters, 3 .gz characters

  // variants/data/projectName/modelName/yyyy/MM/dd/hh/improve-variants-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `variants/data/${projectName}/${modelName}/${year}/${month}/${day}/${hour}/improve-variants-${year}-${month}-${day}-${hour}-${minute}-${second}-${firehoseUuid}.gz`
}
    
module.exports.getHistoryS3Key = (projectName, shardId, earliestEventAt, firehoseS3Key) => {
  if (isNaN(earliestEventAt)) {
    throw `invalid earliestEventAt ${JSON.stringify(earliestEventAt)}`
  }
  
  const pathDatePart = dateFormat.asString("yyyy/MM/dd/hh", earliestEventAt)
  const filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss", earliestEventAt)
  const firehoseUuid = firehoseS3Key.substring(firehoseS3Key.length-39, firehoseS3Key.length-3) // 36 uuid characters, 3 .gz characters

  // histories/data/projectName/shardCount/shardId/yyyy/MM/dd/hh/improve-events-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `histories/data/${projectName}/${SHARD_COUNT}/${shardId}/${pathDatePart}/improve-events-${SHARD_COUNT}-${shardId}-${filenameDatePart}-${firehoseUuid}.gz`
}

module.exports.getHistoryShardS3KeyPrefix = (historyS3Key) => {
  const [histories, data, projectName, shardCount, shardId, year, month, day, hour, historyFileName] = historyS3Key.split('/')

  return `histories/data/${projectName}/${SHARD_COUNT}/${shardId}`
}

module.exports.getStaleHistoryS3Key = (s3Key) => {
  return `histories/janitor/stale/${s3Key.substring("histories/data/".length)}`
}

module.exports.getProjectNameFromHistoryS3Key = (historyS3Key) => {
  return historyS3Key.split('/')[2]
}

module.exports.getJoinedS3Key = (historyS3Key, modelName) => {
  const [histories, data, projectName, shardCount, shardId, year, month, day, hour, historyFileName] = historyS3Key.split('/')
  const joinedFileName = `improve-joined${historyFileName.substring('improve-events'.length)}`
  
  // joined/data/projectName/modelName/shardCount/(train|validation)/(trainSplit|validationSplit)/yyyy/MM/dd/hh/improve-joined-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `joined/data/${projectName}/${modelName}/${shardCount}/${getTrainValidationPathPart(joinedFileName)}/${year}/${month}/${day}/${hour}/${joinedFileName}`
}

module.exports.getJoinedS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/joined/${projectName}/${modelName}/${SHARD_COUNT}`
}

module.exports.getJoinedTrainS3Uri = (projectName, modelName) => {
  return `${me.getJoinedS3Uri(projectName, modelName)}/${me.getTrainPathPart()}`
}

module.exports.getJoinedValidationS3Uri = (projectName, modelName) => {
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
  return `s3://${process.env.RECORDS_BUCKET}/transformed/${projectName}/${model}/${SHARD_COUNT}`
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
