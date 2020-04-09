const dateFormat = require('date-format')
const mmh3 = require('murmurhash3js')

const me = module.exports

/*
  All naming in this system is designed to allow idempotent re-processing of files at any point in the processing pipeline without creating duplicate data downstream.  For this reason
  pipelines are seperated by SHARD_COUNT.  If the SHARD_COUNT is changed then a new processing pipeline will be created to ensure that s3 keys stay consistent and idempotent.
  
  History files are further seperated by the HISTORY_FILE_WINDOW
  
  joined/transformed files are further seperated by the VALIDATION_PROPORTION
  
  As long as these variables stay constant, re-processing of firehose files should be idempotent
*/

// using power of 2 bit string shards makes future re-sharding easier than a modulo based approach
const SHARD_COUNT = 2**Math.floor(Math.log2(process.env.HISTORY_SHARD_COUNT)) // round to floor power of 2

// intentionally not including this in the configuration. changing it would technically require regenerating all files from the firehose files since this would
// change which history files some events are assigned to.  In practice, changing this window shouldn't change performance much unless a lot of delayed events were coming in from client SDKs.
// the reward windows and shard count are much more important for controlling performance.  The maximum buffer window for firehose is 15 minutes so this should catch most events.
const HISTORY_FILE_WINDOW = 3600

// the history file is named based on its earliest event. Only allow events within the window in the file.
module.exports.getHistoryFileWindowMillis = () => {
  return HISTORY_FILE_WINDOW * 1000
}

module.exports.getShardId = (userId) => {
  const bitCount = Math.floor(Math.log2(SHARD_COUNT))
  // generate a 32 bit bit string and truncate so that we're possibly forward compatible with different subspace queries and re-sharding logic
  // a modulo based approach would require a complete remapping every time the number of shards changes
  // only depends on the userId to allow files to moved between projects and models with the same shard count and still avoid duplicates if the firehose file is reprocessed
  return mmh3.x86.hash32(userId).toString(2).padStart(32, '0').substring(0, bitCount)
}
    
module.exports.getHistoryS3Key = (projectName, shardId, earliestEventAt, firehoseS3Key) => {
  const pathDatePart = dateFormat.asString("yyyy/MM/dd/hh", earliestEventAt)
  const filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss", earliestEventAt)

  // histories/data/projectName/shardCount/shardId/yyyy/MM/dd/hh/improve-events-projectName-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `histories/data/${projectName}/${SHARD_COUNT}/${shardId}/${pathDatePart}/improve-events-${projectName}-${SHARD_COUNT}-${shardId}-${filenameDatePart}-${firehoseUuid}`
}

module.exports.getHistoryS3KeyPrefix = (projectName, shardId) => {
  // histories/projectName/hashedUserId/improve-v5-pending-events-projectName-hashedUserId-yyyy-MM-dd-hh-mm-ss-uuid.gz
  // histories/events/projectName/shardCount/shardId/yyyy/MM/dd/hh/improve-events-projectName-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `histories/data/${projectName}/${SHARD_COUNT}/${shardId}`
}

module.exports.getStaleHistoryS3Key = (s3Key) => {
  return `histories/janitor/stale/${s3Key.substring("histories/data/".length)}`
}

module.exports.getJoinedS3Key = (projectName, modelName, historyS3Key) => {
  // joined/data/projectName/modelName/shardCount/(train|validation)/(trainSplit|validationSplit)/yyyy/MM/dd/hh/improve-joined-projectName-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `joined/${projectName}/${modelName}/${SHARD_COUNT}/${getTrainValidationPathPart(fileName)}/${fileName}`
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
