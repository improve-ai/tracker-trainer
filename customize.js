'use strict';


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

