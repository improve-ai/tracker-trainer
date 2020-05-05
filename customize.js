'use strict';

const yaml = require('js-yaml');
const fs   = require('fs');

// Allows incoming tracked data to be routed to different projects
// Authentication information such as AWS Cognito IDs or AWS API Gateway api keys could be used to determine which project the data belongs to 
module.exports.projectNameForTrack = (lambdaEvent, lambdaContext) => {
  // return Object.keys(module.exports.config.projects)[0]
  if (lambdaEvent.requestContext.identity.apiKey === "lRgX7U2VPZ6I1DUaSUr6D8jH4iFju3MY7i3p9mbq") {
    return "bible"
  }
  if (lambdaEvent.requestContext.identity.apiKey === "lF8yFNYXiT5fIlBHQMgbY3EtPUfbjJmS1OskfqiT") {
    return "mindful"
  }
}

// project names are persisted to firehose files as they are ingested. To re-process old firehose files for which the project names have changed,
// implement this method in order to migrate the names.
module.exports.migrateProjectName = (projectName) => {
  if (projectName === "lRgX7U2VPZ6I1DUaSUr6D8jH4iFju3MY7i3p9mbq") {
    return "bible"
  } if (projectName === "lF8yFNYXiT5fIlBHQMgbY3EtPUfbjJmS1OskfqiT") {
    return "mindful"
  }
  
  return projectName
}

// do not modify timestamps or reward windows might no longer be valid
module.exports.modifyHistoryRecords = (projectName, historyRecords) => {
  return historyRecords;
}

module.exports.modifyRewardedAction = (projectName, rewardedAction) => {
  return rewardedAction;
}

// may return null or an array of action records.
// inferredActionRecords may be null or an array
// modifications to timestamp or history_id will be ignored
module.exports.actionRecordsFromHistoryRecord = (projectName, historyRecord, inferredActionRecords) => {
  if (inferredActionRecords) {
    return inferredActionRecords;
  }
  // backwards compatibility with Improve v4
  if (historyRecord.record_type === "using") {
    historyRecord.action = "Message Chosen"
    return historyRecord
  }
}

// may return null or a single rewards record.
// modifications to timestamp or history_id will be ignored
module.exports.rewardsRecordFromHistoryRecord = (projectName, historyRecord) => {
  // if the history record has a "rewards" property, then it is a rewards record
  if (historyRecord.rewards) {
    return historyRecord;
  }
}

module.exports.config = yaml.safeLoad(fs.readFileSync('./customize.yml', 'utf8'));

console.log(JSON.stringify(module.exports.config));