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
  if (lambdaEvent.requestContext.identity.apiKey === "xScYgcHJ3Y2hwx7oh5x02NcCTwqBonnumTeRHThI") {
    return "test"
  }
}

// project names are persisted to firehose files as they are ingested. To re-process old firehose files for which the project names have changed,
// implement this method in order to migrate the names.  Project names are not migrated for data already processed into the records bucket
module.exports.migrateProjectName = (projectName) => {
  if (projectName === "lRgX7U2VPZ6I1DUaSUr6D8jH4iFju3MY7i3p9mbq") {
    return "bible"
  }
  if (projectName === "lF8yFNYXiT5fIlBHQMgbY3EtPUfbjJmS1OskfqiT") {
    return "mindful"
  }
  
  return projectName
}

// do not modify timestamps or reward windows might no longer be valid
module.exports.modifyHistoryRecords = (projectName, historyRecords) => {
  return historyRecords;
}

module.exports.modifyRewardedDecision = (projectName, rewardedDecision) => {
  return rewardedDecision;
}

// may return null or an array of decision records.
// inferredDecisionRecords may be null or an array
// modifications to timestamp or history_id will be ignored
module.exports.decisionRecordsFromHistoryRecord = (projectName, historyRecord, inferredDecisionRecords) => {
  if (inferredDecisionRecords) {
    return inferredDecisionRecords;
  }
  // backwards compatibility with Improve v4
  if (historyRecord.record_type === "using") {
    historyRecord.variant = historyRecord.properties
    historyRecord.namespace = "messages"
    return historyRecord
  }
}


module.exports.config = yaml.safeLoad(fs.readFileSync('./customize.yml', 'utf8'));