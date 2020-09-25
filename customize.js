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
    if (r.model && r.model === "messages-1.0" && r.variant) {
      const messageDecision = {...r}
      messageDecision.model = "messages-2.0"
      messageDecision.variant = r.variant.message
      
      const themeDecision = {...r}
      themeDecision.model = "themes-2.0"
      themeDecision.variant = r.variant.theme
      // set the message on the context
      if (!themeDecision.context) {
        themeDecision.context = {}
      }
      themeDecision.context.message = r.variant.message
      
      results.append(messageDecision)
      results.append(themeDecision)
    } else {
      results.append(r)
    }
  })

  return results
}

module.exports.config = yaml.safeLoad(fs.readFileSync('./customize.yml', 'utf8'));