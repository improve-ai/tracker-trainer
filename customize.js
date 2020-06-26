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

// return records of .type equal to "decision", "rewards", or "propensity"
// must include "type", timestamp", "message_id", and "history_id" fields
module.exports.customizeRecords = (projectName, records) => {
  const results = []
  records.forEach(record => {
    if (record.type === "decision" || record.type === "rewards" || record.type === "propensity") {
      results.push(record)
      return
    }
    
    // improve v4 share rewards
    if (record.record_type == "rewards") { 
      record.type = "rewards"
      results.push(record)
    }
    
    // migrate improve v4 using record
    if (record.record_type === "using" && record.properties) { 
      if (record.context) {
        delete record.context.shared // delete old shared
      }
  
      if (record.properties.message) {
        let messageDecision = { type: "decision" }
        messageDecision.namespace = "messages"
        messageDecision.timestamp = record.timestamp
        messageDecision.history_id = record.history_id
        messageDecision.message_id = record.message_id
        messageDecision.variant = record.properties.message
        messageDecision.context = record.context
        messageDecision.reward_key = record.reward_key // pass through v4 reward key
        
        results.push(messageDecision)
      }
      
      if (record.properties.theme) {
        let themeDecision = { type: "decision" }
        themeDecision.namespace = "themes"
        themeDecision.timestamp = record.timestamp
        themeDecision.history_id = record.history_id
        themeDecision.message_id = `${record.message_id}-1`
        themeDecision.variant = record.properties.theme
  
        themeDecision.context = record.context
        
        // add message as context
        if (!themeDecision.context) {
          themeDecision.context = {}
        }
        themeDecision.context.message = record.properties.message
  
        themeDecision.reward_key = record.reward_key // pass through v4 reward key
        results.push(themeDecision)
      }
    }
  })
  
  return results
}

module.exports.config = yaml.safeLoad(fs.readFileSync('./customize.yml', 'utf8'));