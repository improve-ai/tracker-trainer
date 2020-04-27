'use strict';

const mmh3 = require('murmurhash3js')
const uuidv4 = require('uuid/v4')
const s3utils = require("./s3utils.js")
const customize = require("./customize.js")

const me = module.exports

module.exports.getHistoryS3Key = (projectName, shardId, timestamp, uuid) => {
  // ensure that we're using UTC
  const [year, month, day] = timestamp.toISOString().slice(0,10).split('-')

  // histories/data/projectName/shardId/yyyy/MM/dd/improve-events-shardId-yyyy-MM-dd-uuid.gz
  return `histories/data/${projectName}/${shardId}/${year}/${month}/${day}/improve-events-${shardId}-${year}-${month}-${day}-${uuid}.gz`
}

module.exports.getShardIdForS3Key = (s3Key) => {
  if (me.isHistoryS3Key(s3Key)) {
    return s3Key.split('/')[3]
  } else if (me.isIncomingHistoryS3Key(s3Key)) {
    return s3Key.split('/')[4]
  } else if (me.isRewardedActionS3Key(s3Key)) {
    return s3Key.split('/')[6]
  }
  
  throw new Error(`s3Key ${s3Key} is not a history, incoming history marker, or rewarded action key`)
}

module.exports.replaceShardIdForS3Key = (s3Key, newShardId, timestamp) => {
  if (me.isHistoryS3Key(s3Key)) {
    return me.replaceShardIdForHistoryS3Key(s3Key, newShardId)
  } else if (me.isRewardedActionS3Key(s3Key)) {
    return me.replaceShardIdForRewardedActionS3Key(s3Key, newShardId, timestamp)
  }

  throw new Error(`s3Key ${s3Key} is not a history or rewarded action key`)
}

module.exports.replaceShardIdForHistoryS3Key = (historyS3Key, newShardId) => {
  if (!me.isHistoryS3Key(historyS3Key)) {
    throw new Error(`parentS3Key ${historyS3Key} is not a history or rewarded action key`)
  }
  const split  = historyS3Key.split('/')
  split[3] = newShardId
  return split.join('/')
}

module.exports.getVariantsS3Key = (projectName, modelName, firehoseS3Key) => {
  const dashSplitS3Key = firehoseS3Key.split('-')
  const [year, month, day, hour, minute, second] = dashSplitS3Key.slice(dashSplitS3Key.length-11, dashSplitS3Key.length - 5) // parse from the back to avoid unexpected dashes
  const firehoseUuid = firehoseS3Key.substring(firehoseS3Key.length-39, firehoseS3Key.length-3) // 36 uuid characters, 3 .gz characters

  // variants/data/projectName/modelName/yyyy/MM/dd/improve-variants-yyyy-MM-dd-hh-mm-ss-firehoseUuid.gz
  return `variants/data/${projectName}/${modelName}/${year}/${month}/${day}/improve-variants-${year}-${month}-${day}-${hour}-${minute}-${second}-${firehoseUuid}.gz`
}

module.exports.isHistoryS3Key = (s3Key) => {
  return s3Key.startsWith("histories/data/")
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
  return `histories/meta/incoming/${s3Key.substring("histories/data/".length)}.json`
}

module.exports.getIncomingHistoryS3KeyPrefix = (projectName) => {
  return `histories/meta/incoming/${projectName}/`
}

module.exports.getIncomingHistoryShardS3KeyPrefix = (projectName, shardId) => {
  return `histories/meta/incoming/${projectName}/${shardId}/`
}

module.exports.getProjectNameFromHistoryS3Key = (historyS3Key) => {
  return historyS3Key.split('/')[2]
}

module.exports.getShardTimestampsS3KeyPrefix = (projectName) => {
  return `histories/meta/shard_timestamps/${projectName}/`
}

module.exports.getUniqueShardTimestampsS3Key = (projectName) => {
  return `${me.getHistoryS3KeyPrefix(projectName)}shard-timestamps-${uuidv4}.json`
}

module.exports.isRewardedActionS3Key = (s3Key) => {
  return s3Key.startsWith("rewarded_actions/data")
}

module.exports.getRewardActionProjectS3KeyPrefix = (projectName) => {
  return `rewarded_actions/data/${projectName}/`
}

module.exports.getRewardedActionS3Key = (projectName, modelName, shardId, timestamp) => {
    // ensure that we're using UTC
  const [year, month, day] = timestamp.toISOString().slice(0,10).split('-')

  const fileName = `improve-actions-${shardId}-${year}-${month}-${day}.gz`
  
  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return `rewarded_actions/data/${projectName}/${modelName}/${getTrainValidationPathPart(fileName)}/${shardId}/${year}/${month}/${day}/${fileName}`
}

module.exports.replaceShardIdForRewardedActionS3Key = (s3Key, newShardId, timestamp) => {
  if (!me.isRewardedActionS3Key(s3Key)) {
    throw new Error(`s3Key ${s3Key} is not a rewarded action s3 key`)
  }
  const [rewardActions, data, projectName, modelName, trainOrValidation, split, shardId, year, month, day, fileName] = s3Key.split('/')
  
  // changing the shardId changes the train/validation part so we must re-hash rather than just replace the shardId
  return me.getRewardedActionS3Key(projectName, modelName, newShardId, timestamp)
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


// Implements the train/validation split based on the hash of the file name.  
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

module.exports.listAllShardTimestampsS3Keys = (projectName) => {
  console.log(`listing timestamp keys`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getShardTimestampsS3KeyPrefix(projectName)
  }

  return s3utils.listAllKeys(params)
}


module.exports.listAllHistoryShardS3Keys = (projectName, shardId) => {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getHistoryShardS3KeyPrefix(projectName, shardId)
  }
  
  return s3utils.listAllKeys(params)
}

module.exports.listAllIncomingHistoryShardS3Keys = (projectName, shardId) => {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getIncomingHistoryShardS3KeyPrefix(projectName, shardId)
  }

  return s3utils.listAllKeys(params)
}

module.exports.listSortedShardsByProjectName = () => {
  console.log(`listing shards`)
  const projectNames = Object.keys(customize.getProjectNamesToModelNamesMapping())
  return Promise.all(projectNames.map(projectName => {
    return Promise.all([me.listAllHistoryShards(projectName), me.listAllRewardedActionShards(projectName)]).then(all => all.flat()).then(shardIds => {
      shardIds = [...new Set(shardIds)] // de-duplicate since we'll see the same shards in history and the rewarded actions
      shardIds.sort() // sort the shards
      console.log(`shardIds ${JSON.stringify(shardIds)}`)
      return [projectName, shardIds]
    })
  })).then(projectNamesAndShardIds => {
    const shardsByProjectNames = {}
    for (const [projectName, sortedShardIds] of projectNamesAndShardIds) {
      shardsByProjectNames[projectName] = sortedShardIds
    }
    return shardsByProjectNames
  })
}

module.exports.listAllHistoryShards = (projectName) => {
  console.log(`listing shards for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getHistoryS3KeyPrefix(projectName)
  }
  
  return s3utils.listAllSubPrefixes(params)
}

module.exports.listAllIncomingHistoryShards = (projectName) => {
  console.log(`listing shards for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getIncomingHistoryS3KeyPrefix(projectName)
  }
  
  return s3utils.listAllSubPrefixes(params)
}

module.exports.listAllRewardedActionShards = (projectName) => {
  console.log(`listing models for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getRewardActionProjectS3KeyPrefix(projectName)
  }

  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return s3utils.listAllPrefixes(params, 4).then(prefixes => prefixes.map(prefix => prefix.split('/').pop())).then(shards => {
    return [...new Set(shards)] // de-duplicate since shards can exist across models and train/validation splits
  })
}

module.exports.listAllRewardedActionShardS3Keys = (projectName, shardId) => {
  // this will return a few extra prefixes because not all shards will be in all prefixes due to model splits and train/validation splits
  return me.listAllRewardedActionShardS3KeyPrefixes(projectName, shardId).then(prefixes => {
    return Promise.all(prefixes.map(prefix => {
      const params = {
        Bucket: process.env.RECORDS_BUCKET,
        Prefix: prefix
      }
      
      return s3utils.listAllKeys(params)
    })).then(all => {
      return all.flat()
    })
  })
}

module.exports.listAllRewardedActionShardS3KeyPrefixes = (projectName, shardId) => {
  console.log(`listing models for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getRewardActionProjectS3KeyPrefix(projectName)
  }

  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return s3utils.listAllPrefixes(params, 3)
}
