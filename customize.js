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
                hosting_initial_instance_count: 2
            } 
        },
        lF8yFNYXiT5fIlBHQMgbY3EtPUfbjJmS1OskfqiT: {
            "messages-1.0": {
                max_age: "15552000", // 180 days
                hosting_initial_instance_count: 1
            } 
        }
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

// This function may also return a promise for performing asynchronous processing
module.exports.assignRewardedActionsToModelsFromHistoryEvents = (projectName, sortedHistoryEvents) => {
    const modelsToJoinedEvents = {}
    const rewardKeysToEvents = {}
    
    for (const record of sortedHistoryEvents) {
        if (record.record_type) {
            if (record.record_type === "using" && record.reward_key) {
                record.reward = 0
                if (!modelsToJoinedEvents[record.model]) {
                    modelsToJoinedEvents[record.model] = []
                }
                modelsToJoinedEvents[record.model].push(record)
                rewardKeysToEvents[record.reward_key] = record
            } else if (record.record_type === "rewards" && record.rewards) {
                for (const [rewardKey, reward] of Object.entries(record.rewards)) {
                    if (rewardKey in rewardKeysToEvents) {
                        rewardKeysToEvents[rewardKey].reward = rewardKeysToEvents[rewardKey].reward + reward
                    }
                }
            }
        }
    }
    
    return modelsToJoinedEvents;
}

