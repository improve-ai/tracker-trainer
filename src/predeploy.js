const assert = require('assert')
const fs = require('fs')

let yaml = null

try {
  yaml = require('yaml')
} catch (error) {
  fatal("'yaml' not found, please run 'npm install' before deploying")
}

// Unit-tested in tests/test_regexps.py:test_predeploy_org_and_project_name_regexp()
const orgAndProjNameRegex = '^[a-z0-9]+$'
// Unit-tested in tests/test_regexps.py:test_model_name_regexp()
const modelNameRegex = /^[a-zA-Z0-9][\w\-.]{0,63}$/i

// perform the primary configuration and set module.exports variables
function configure() {

  const config = yaml.parse(fs.readFileSync('./config/config.yml', 'utf8'))

  // assert organization and project may contain only lowercase letters and
  // numbers and must be non-null, non-empty strings
  const organization = config['organization']
  const project = config['project']
  
  assert(organization != null, 'config/config.yml:organization is null or undefined')
  assert(project != null, 'config/config.yml:project is null or undefined')
  
  if(organization == 'acme'){
    warn("config/config.yml:organization - currently detected example organization => 'acme', please change")
  }
  
  assert(organization.match(orgAndProjNameRegex), 'config/config.yml:organization may contain only lowercase letters and numbers')
  assert(project.match(orgAndProjNameRegex), 'config/config.yml:project may contain only lowercase letters and numbers')
  
  assert(organization != '', 'config/config.yml:organization is an empty string')
  assert(project != '', 'config/config.yml:project is an empty string')
  
  const models = config['models'] || {}
  assert(isDict(models), 'config/config.yml:models is not a dictionary')
  
  const events = []
  const trainingDefaults = config['training'] || {}

  for (let [modelName, modelConfig] of Object.entries(models)) {

    assert(modelName.match(modelNameRegex), `invalid model name: ${modelName}`)

    const event = {}
    const eventSchedule = {}
    event['schedule'] = eventSchedule

    const eventScheduleInput = {}
    eventSchedule['input'] = eventScheduleInput

    eventSchedule['enabled'] = true

    // set rule name. serverless interpolates the ${opt:stage...} part
    // TODO can't be more than 64 characters
    eventSchedule['name'] = `improveai-${config['organization']}-${config['project']}-` + '${opt:stage, self:provider.stage}' + `-${modelName}`

    modelConfig = modelConfig || {}
    assert(isDict(modelConfig), `config/config.yml:models.${modelName} is not a dictionary`)

    const modelTrainingConfig = modelConfig['training'] || {}

    // merge the model config, falling back to the defaults
    const trainingConfig = Object.assign({}, trainingDefaults, modelTrainingConfig)

    // merge hyperparameters
    trainingConfig['hyperparameters'] =  Object.assign({}, trainingDefaults['hyperparameters'], modelTrainingConfig['hyperparameters'])

    // assign hyperparameters
    eventScheduleInput['hyperparameters'] = trainingConfig['hyperparameters']

    // pass scheduling info
    eventSchedule['rate'] = trainingConfig['schedule']

    // pass description
    eventSchedule['description'] = `invoke GroomThenTrain step function for ${modelName} model`

    // pass env vars as parameters
    eventScheduleInput['model_name'] = modelName

    eventScheduleInput['instance_type'] = trainingConfig['instance_type']
    eventScheduleInput['instance_count'] = trainingConfig['instance_count']
    eventScheduleInput['max_runtime'] = parseMaxRuntimeString(trainingConfig['max_runtime'])
    eventScheduleInput['volume_size'] = parseVolumeSize(trainingConfig['volume_size'])

    events.push(event)
  }
  
  if (events.length == 0) {
    warn('config/config.yml:models - no models configured, so none will be trained')
  }
  
  module.exports.trainingScheduleEvents = events
}

function isDict(x) {
  return x && x.constructor === Object
}


function fatal(msg) {
  console.error(`[FATAL] ${msg}`)
  throw msg
}


function warn(msg) {
  console.warn(`[WARNING] ${msg}`)
}


function checkFixAndSplitValueWithUnit(checkedString, emptyOrNullStringError, checkedParameterName) {
  assert(!(checkedString == null), emptyOrNullStringError)
  assert(!(checkedString == ''), emptyOrNullStringError)
  // replace multiple spaces, tabs, etc with single space
  var checkedStringFixed = checkedString.toString().replace(/\s\s+/g, ' ').trim()
  // split on space to separate value and unit
  var checkedStringArray = checkedStringFixed.split(' ')

  assert(checkedStringArray.length == 2, `${checkedParameterName} has bad format`)
  return checkedStringArray
}


function parseMaxRuntimeString(maxRuntimeString) {

  const MAX_RUNTIME_UNITS_TO_SECONDS = { seconds: 1, minutes: 60, hours: 3600, days: 86400 }

  var maxRuntimeParameterName = 'max_runtime'
  var maxRuntimeArray = checkFixAndSplitValueWithUnit(
    maxRuntimeString, 'max_runtime must not be empty', maxRuntimeParameterName)
  var maxRuntimeUnit = maxRuntimeArray[1].toLowerCase()

  assert(Object.keys(MAX_RUNTIME_UNITS_TO_SECONDS).includes(maxRuntimeUnit), "time unit must be one of 'seconds', 'minutes', 'hours', 'days'")

  var maxRuntimeValue = -1

  try {
    maxRuntimeValue = parseInt(maxRuntimeArray[0])
  } catch (error) {
    throw 'unable to max_runtime'
  }

  assert(maxRuntimeValue > 0, 'max_runtime must be > 0')
  return maxRuntimeValue * MAX_RUNTIME_UNITS_TO_SECONDS[maxRuntimeUnit]
}


function parseVolumeSize(volumeSizeString) {

  var volumeSizeParameterName = 'volume_size'
  var volumeSizeArray = checkFixAndSplitValueWithUnit(
    volumeSizeString, 'volume_size must not be empty', volumeSizeParameterName)
  var volumeSizeUnit = volumeSizeArray[1]

  assert(volumeSizeUnit == 'GB', "volume_size unit must be 'GB'")

  var volumeSizeValue = -1

  volumeSizeValue = parseFloat(volumeSizeArray[0])
  assert(volumeSizeValue == Math.floor(volumeSizeValue), 'volume_size must be an integer')

  assert(volumeSizeValue > 0, 'volume_size must be > 0')
  // returning volume size GBs
  return volumeSizeValue
}

// execute configuration and set module.exports.trainingSchedulingEvents
configure()
