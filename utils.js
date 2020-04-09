const mmh3 = require('murmurhash3js')
const me = module.exports

// using power of 2 bit string shards makes future re-sharding easier than a modulo based approach
const SHARD_COUNT = 2**Math.floor(Math.log2(process.env.HISTORY_SHARD_COUNT)) // round to floor power of 2

module.exports.getShardId = (projectName, userId) => {
  const bitCount = Math.floor(Math.log2(SHARD_COUNT))
  // generate a 32 bit bit string and truncate so that we're possibly forward compatible with different subspace queries and re-sharding logic
  return mmh3.x86.hash32(projectName.length + ":" + projectName + ":" + userId).toString(2).padStart(32, '0').substring(0, bitCount)
}
    
module.exports.getHistoryS3Key = (projectName, shardId, earliestEventAt, firehoseUuid) => {
  // histories/data/projectName/shardCount/shardId/yyyy/MM/dd/hh/improve-events-projectName-shardCount-shardId-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `histories/data/${projectName}/${SHARD_COUNT}/${shardId}/improve-events-${SHARD_COUNT}-${shardId}-${firehoseUuid}`
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

function getFeatureModelsS3KeyPrefix(projectName, modelName) {
  return `feature_models/${projectName}/${modelName}`
}

function getXGBoostModelsS3KeyPrefix(projectName, modelName) {
  return `xgboost_models/${projectName}/${modelName}`
}
