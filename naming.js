'use strict';

const assert = require('assert').strict;
const _ = require('lodash')
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

// generate a new history S3 key with unique uuid
module.exports.getConsolidatedHistoryS3Key = (historyS3Key) => {
  assert(me.isHistoryS3Key(historyS3Key), "must be a history S3 Key")
  const [histories, data, projectName, shardId, year, month, day, file] = historyS3Key.split('/')
  return `histories/data/${projectName}/${shardId}/${year}/${month}/${day}/improve-events-${shardId}-${year}-${month}-${day}-${uuidv4()}.gz`
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

// TODO not properly replacing the shard id in the file name
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

module.exports.replaceShardIdForRewardedActionS3Key = (s3Key, newShardId, timestamp) => {
  if (!me.isRewardedActionS3Key(s3Key)) {
    throw new Error(`s3Key ${s3Key} is not a rewarded action s3 key`)
  }
  const [rewardActions, data, projectName, modelName, trainOrValidation, split, shardId, year, month, day, fileName] = s3Key.split('/')
  
  // changing the shardId changes the train/validation part so we must re-hash rather than just replace the shardId
  return me.getRewardedActionS3Key(projectName, modelName, newShardId, timestamp)
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

module.exports.groupHistoryS3KeysByDatePath = (historyS3Keys) => {
  return _.groupBy(historyS3Keys, (s3Key) => s3Key.split('/').slice(0,7).join('/'))
}

module.exports.isIncomingHistoryS3Key = (s3Key) => {
  return s3Key.startsWith("histories/meta/incoming/")
}

module.exports.getIncomingHistoryS3Key = (s3Key) => {
  if (!me.isHistoryS3Key(s3Key)) {
    throw new Error(`s3Key ${s3Key} must be an history s3 key`)
  }
  return `histories/meta/incoming/${s3Key.substring("histories/data/".length)}.json`
}

module.exports.getHistoryS3KeyForIncomingHistoryS3Key = (s3Key) => {
  if (!me.isIncomingHistoryS3Key(s3Key)) {
    throw new Error(`s3Key ${s3Key} must be an incoming history s3 key`)
  }
  return `histories/data/${s3Key.substring("histories/meta/incoming/".length, s3Key.length-".json".length)}`
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
  return `${me.getShardTimestampsS3KeyPrefix(projectName)}shard-timestamps-${uuidv4()}.json`
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

module.exports.getRewardedActionS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/rewarded_actions/data/${projectName}/${modelName}`
}

module.exports.getRewardedActionTrainS3Uri = (projectName, modelName) => {
  return `${me.getRewardedActionS3Uri(projectName, modelName)}/${me.getTrainPathPart()}`
}

module.exports.getRewardedActionValidationS3Uri = (projectName, modelName) => {
  return `${me.getRewardedActionS3Uri(projectName, modelName)}/${me.getValidationPathPart()}`
}


// Implements the train/validation split based on the hash of the file name.  
function getTrainValidationPathPart(fileName) {
  let validationProportion = parseFloat(process.env.VALIDATION_PROPORTION)
  
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
  return `${me.getTransformedS3Uri(projectName, modelName)}${me.getTrainPathPart()}`
}

module.exports.getTransformedValidationS3Uri = (projectName, modelName) => {
  return `${me.getTransformedS3Uri(projectName, modelName)}${me.getValidationPathPart()}`
}

module.exports.getFeatureModelsS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/feature_models/${projectName}/${modelName}`
}

module.exports.getXGBoostModelsS3Uri = (projectName, modelName) => {
  return `s3://${process.env.RECORDS_BUCKET}/xgboost_models/${projectName}/${modelName}`
}

module.exports.allProjects = () => {
  return Object.keys(customize.config.projects)
}

module.exports.getModelsByProject = () => {
  return Object.fromEntries(Object.entries(customize.config.projects).map(([project, projectDict]) => [project, Object.keys(projectDict.models)]))
}

module.exports.getModelForAction = (projectName, action) => {
  if (!customize.config.projects || !customize.config.projects[projectName]) {
    throw new Error("no configured project ${projectName}")
  }

  let catchallModel;
  const modelConfigs = customize.config.projects[projectName].models
  for (const [model, modelConfig] of Object.entries(modelConfigs)) {
    if (!me.isValidModelName(model)) {
      throw new Error(`invalid model name ${model}, not alphanumeric, underscore, dash, space, period`)
    }
    // there is only one catchall model
    if (!modelConfig || !modelConfig.actions || modelConfig.actions.length == 0) {
      if (catchallModel) {
        throw new Error(`only one catchall model (zero \"actions\") can be configured per project ${projectName} - ${JSON.stringify(modelConfigs)}`)
      }
      catchallModel = model
    } else {
      // check to see if this action is explicitly handled by a model
      for (const acceptedAction of modelConfig.actions) {
        if (acceptedAction === action) {
          return model
        }
      }
    }
  }

  // this action is not explicitly configured. Use the catchall model
  return catchallModel
}

// TODO
module.exports.getXGBoostHyperparameters = (projectName, model) => {
  const hyperparameters = {} 
  if (customize.config.binaryRewards) {
    Object.assign(hyperparameters,  { objective: "binary:logistic" })
  }
  
  return Object.assign(hyperparameters, customize.config.xgboostHyperparameters)
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

module.exports.isObjectNotArray = (value) => {
  return _.isObject(value) && !Array.isArray(value)
}

module.exports.assertValidRewardedAction = (ra) => {
  assert(_.isString(ra.history_id), "history_id must be string")
  assert(_.isString(ra.message_id), "message_id must be string")
  assert(_.isString(ra.timestamp), "timestamp must be string")
  assert(me.isObjectNotArray(ra.properties), "properties must be a dictionary")
  if (ra.context) {
    assert(me.isObjectNotArray(ra.context), "context must be a dictionary")
  }
  if (ra.action) {
    assert(_.isString(ra.action),"action must be string")
  }
  if (ra.reward) {
    assert(_.isFinite(ra.reward))
  }
}

module.exports.getLambdaFunctionArn = (functionName, invokedFunctionArn) => {
  // arn:aws:lambda:us-west-2:117097735164:function:improve-v5-test-firehoseFileCreated
  const splitted = invokedFunctionArn.split('-')
  return `${splitted.slice(0,splitted.length-1).join('-')}-${functionName}`
}

module.exports.listAllShardTimestampsS3Keys = (projectName) => {
  console.log(`listing timestamp keys for project ${projectName}`)
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

module.exports.listAllHistoryShardS3KeysMetadata = (projectName, shardId) => {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getHistoryShardS3KeyPrefix(projectName, shardId)
  }
  
  return s3utils.listAllKeysMetadata(params)
}


module.exports.listAllIncomingHistoryShardS3Keys = (projectName, shardId) => {
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Prefix: me.getIncomingHistoryShardS3KeyPrefix(projectName, shardId)
  }

  return s3utils.listAllKeys(params)
}

module.exports.listAllRewardedActionShardS3Keys = (projectName, shardId) => {
  // this will return a few extra prefixes that won't contain keys because not all shards will be in all prefixes due to model splits and train/validation splits
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

module.exports.listSortedShardsByProjectName = () => {
  const projectNames = me.allProjects()
  return Promise.all(projectNames.map(projectName => {
    return me.listAllShards(projectName).then(shards => [projectName, shards])
  })).then(projectNamesAndShardIds => {
    const shardsByProjectNames = {}
    for (const [projectName, shardIds] of projectNamesAndShardIds) {
      shardsByProjectNames[projectName] = shardIds.sort()
    }
    return shardsByProjectNames
  })
}

module.exports.listAllShards = (projectName) => {
  // TODO fetch transformed shards
  console.log(`listing all shards for project ${projectName}`)
  return Promise.all([me.listAllHistoryShards(projectName), me.listAllIncomingHistoryShards(projectName), me.listAllRewardedActionShards(projectName)]).then(all => all.flat()).then(shardIds => {
    shardIds = [...new Set(shardIds)] // de-duplicate since we'll see the same shards in history and the rewarded actions
    console.log(`project ${projectName} shards ${JSON.stringify(shardIds)}`)
    // TODO check valid shard id, no "" or non binary
    return shardIds
  })
}

module.exports.listAllHistoryShards = (projectName) => {
  console.log(`listing history shards for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getHistoryS3KeyPrefix(projectName)
  }
  
  return s3utils.listAllSubPrefixes(params).then(shards => {
    console.log(`history shards project ${projectName} shards ${JSON.stringify(shards)}`)
    return shards
  })
}

module.exports.listAllIncomingHistoryShards = (projectName) => {
  if (!projectName) {
    throw new Error("projectName required")
  }
  console.log(`listing incoming history shards for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getIncomingHistoryS3KeyPrefix(projectName)
  }
  
  return s3utils.listAllSubPrefixes(params).then(shards => {
    console.log(`incoming history shards project ${projectName} shards ${JSON.stringify(shards)}`)
    return shards
  })
}

module.exports.listAllRewardedActionShards = (projectName) => {
  console.log(`listing rewarded action shards for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getRewardActionProjectS3KeyPrefix(projectName)
  }
  
  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return s3utils.listAllPrefixes(params, 4).then(prefixes => prefixes.map(prefix => prefix.split('/')[6])).then(shards => {
    console.log(`rewarded action shards project ${projectName} shards ${JSON.stringify(shards)}`)
    return [...new Set(shards)] // de-duplicate since shards can exist across models and train/validation splits
  })
}


module.exports.listAllRewardedActionShardS3KeyPrefixes = (projectName, shardId) => {
  console.log(`listing rewarded action shard prefixes for project ${projectName}`)
  const params = {
    Bucket: process.env.RECORDS_BUCKET,
    Delimiter: '/',
    Prefix: me.getRewardActionProjectS3KeyPrefix(projectName)
  }

  // rewarded_actions/data/projectName/modelName/(train|validation)/(trainSplit|validationSplit)/shardId/yyyy/MM/dd/improve-actions-shardId-yyyy-MM-dd.gz
  return s3utils.listAllPrefixes(params, 4).then(prefixes => prefixes.filter(prefix => prefix.split('/')[6] === shardId))
}
