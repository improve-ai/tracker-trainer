'use strict';

module.exports = {
    hyperparameters: {
        "default": {
            max_age: "7776000", // 90 days
            objective: "binary:logistic",
        },
        lRgX7U2VPZ6I1DUaSUr6D8jH4iFju3MY7i3p9mbq: {
            "messages-1.0": {
                max_age: "3456000", // 40 days
            } 
        },
        lF8yFNYXiT5fIlBHQMgbY3EtPUfbjJmS1OskfqiT: {
            "messages-1.0": {
                max_age: "15552000", // 180 days
            } 
        }
    }
}

module.exports.modelNameForAction = (action) => {
    return "default"
}

module.exports.modifyHistoryRecords = (projectName, historyId, historyRecords) => {
  return historyRecords
}

module.exports.modifyRewardedAction = (projectName, rewardedAction) => {
  return rewardedAction
}

// may return null or an array of action records.
// inferredActionRecords may be null or an array
// any timestamp modifications will be ignored
module.exports.actionRecordsFromHistoryRecord = (projectName, historyRecord, inferredActionRecords) => {
  return inferredActionRecords
}

// may return null or a single rewards record.
// any timestamp modifications will be ignored
module.exports.rewardsRecordFromHistoryRecord = (projectName, historyRecord) => {
  // if the history record has a "rewards" property, then it is a rewards record
  if (historyRecord.rewards) {
    return historyRecord
  }
}

// the default processing in unpack_firehose.js allows alphanumeric, underscore, dash, space, and period in project names
module.exports.getProjectNamesToModelNamesMapping = () => {
    return {
        "lRgX7U2VPZ6I1DUaSUr6D8jH4iFju3MY7i3p9mbq": ["messages-1.0"],
        "lF8yFNYXiT5fIlBHQMgbY3EtPUfbjJmS1OskfqiT": ["messages-1.0"]
    }
}

// Allows user data to be split into different projects
// Authentication information such as Cognito IDs or API Keys could be used to determine which project the data belongs to 
module.exports.getProjectName = (event, context) => {
    // return Object.keys(module.exports.getProjectNamesToModelNamesMapping())[0]
    return event.requestContext.identity.apiKey;
}
