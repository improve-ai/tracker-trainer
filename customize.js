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

// return records array with each record "type" equal to "decision" or "rewards"
// decision records must include "type", "timestamp", "message_id", "history_id", "namespace", and "variant"
// rewards records must include "type", "timestamp", "message_id", "history_id", and "rewards"
module.exports.customizeRecords = (projectName, records) => {
  
  const results = []
  
  records.forEach(r => {
    delete r.shared
    
    // migrate messages-1.0 { message: ..., theme: ... }
    if ((r.namespace === "messages-1.0" || r.model === "messages-1.0") && r.variant) {
      const messageDecision = Object.assign({}, r)
      messageDecision.model = "messages-2.0"
      messageDecision.variant = r.variant.message
      
      const themeDecision = Object.assign({}, r)
      themeDecision.model = "themes-2.0"
      themeDecision.variant = r.variant.theme
      // set the message on the context
      themeDecision.context = Object.assign({}, r.context)
      themeDecision.context.message = r.variant.message
      
      // TODO check valid record
      if (messageDecision.variant) {
        results.push(messageDecision)
      }
      if (themeDecision.variant) {
        results.push(themeDecision)
      }
    } else if (r.type === "rewards") {
      r.type = "event"
      r.event = "Rewards"
      r.properties = { value: 0.02 }
      results.push(r)
    } else if (r.model === "messages-2.0" || r.model === "themes-2.0" || r.model === "stories-2.0" || r.model === "songs-2.0") {
      results.push(r)
    }
  })

  return results
}

module.exports.config = yaml.safeLoad(fs.readFileSync('./customize.yml', 'utf8'));