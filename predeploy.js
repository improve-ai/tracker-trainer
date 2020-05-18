const customize = require("./customize.js") // load customize.js and customize.yml to check for parse errors

module.exports = serverless => {
  serverless.cli.consoleLog('customize.yml passes checks')
  
  // TODO check that there is a model with no actions
  // TODO check that there is at least one project
  // TODO check that each project has at least one model
  return true
}