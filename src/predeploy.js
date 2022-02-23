const assert = require('assert')
const fs = require('fs')

let yaml = null

try {
  yaml = require('yaml')
} catch (error) {
  fatal('Please run `npm install` before deploying')
  throw (error)
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
  
  assert(!(organization == null), 'config/config.yml:organization is null or undefined')
  assert(!(project == null), 'config/config.yml:project is null or undefined')
  
  if(organization == 'acme'){
    warn("config/config.yml:organization - currently detected example organization => 'acme', please change")
  }
  
  assert(organization.match(orgAndProjNameRegex), 'config/config.yml:organization may contain only lowercase letters and numbers')
  assert(project.match(orgAndProjNameRegex), 'config/config.yml:project may contain only lowercase letters and numbers')
  
  assert(organization != '', 'config/config.yml:organization is an empty string')
  assert(project != '', 'config/config.yml:project is an empty string')
  
  if (config['models'] === null) {
  
    warn("no models configured in config/config.yml, no models will be trained.")
  
    return
  } 
  
  assert(isDict(config['models']), "'models' entries should be dictionaries")
  
  // model names should be validated according to model naming rules
  for (const [key, value] of Object.entries(config['models'])) {
    assert(key.match(modelNameRegex), `invalid model name: ${key}`)
  }
  
  module.exports.trainingSchedulingEvents = getTrainingSchedulingEvents(config)
}

function getTrainingSchedulingEvents(config) {
  
  // Apply defaults to this pattern
  const scheduleEventPattern = {
    "schedule": {
        "name": null,
        "description": "default schedule",
        "rate": null,
        "enabled": true,
        "input": null
    }
  }

  const trainingSchedulingEvents = []
  const images = config['images']
  const trainingDefaults = config['training']
  

  //defaults
  var defaultWorkerInstanceType = trainingDefaults['instance_type']
  var defaultWorkerCount = trainingDefaults['instance_count']
  var defaultMaxRuntimeInSeconds = trainingDefaults['max_runtime']
  var defaultVolumeSize = trainingDefaults['volume_size']
  var defaultHyperparameters = trainingDefaults['hyperparameters']

  var currentScheduleEventDef = null
  var currentModelTrainingConfig = {}


  for (const [modelName, modelConfig] of Object.entries(config['models'])) {

    if (modelConfig == null) {
        currentModelTrainingConfig = {}
    } else {
        currentModelTrainingConfig = get(modelConfig, 'training', {})
        if(currentModelTrainingConfig === {}){
            console.warn(`[WARNING] No 'training' section found in model config for model: ${modelName}`)
        }
    }

    const event = {}
    const eventSchedule = {}
    event['schedule'] = eventSchedule
    
    eventSchedule['enabled'] = true

    // set rule name
    eventSchedule['name'] = `improveai-${config['organization']}-${config['project']}-` + '${opt:stage, self:provider.stage}' + `-${modelName}-schedule`
    // pass scheduling info
    eventSchedule['rate'] = get(currentModelTrainingConfig, 'schedule', trainingDefaults['schedule'])

    // pass description
    eventSchedule['description'] = `${eventSchedule['rate']} schedule of ${modelName} model`

    const eventScheduleInput = {}
    eventSchedule['input'] = eventScheduleInput

    // pass env vars as parameters
    eventScheduleInput['model_name'] = modelName
    eventScheduleInput['instance_type'] = get(currentModelTrainingConfig, 'instance_type', defaultWorkerInstanceType)
    eventScheduleInput['instance_count'] = get(currentModelTrainingConfig, 'instance_count', defaultWorkerCount)
    eventScheduleInput['max_runtime'] = parseMaxRuntimeString(get(currentModelTrainingConfig, 'max_runtime', defaultMaxRuntimeInSeconds))
    eventScheduleInput['volume_size'] = parseVolumeSize(get(currentModelTrainingConfig, 'volume_size', defaultVolumeSize))
    eventScheduleInput['hyperparameters'] = get(currentModelTrainingConfig, 'hyperparameters', defaultHyperparameters)
          
    const image = trainingConfig['image']

    if(image == '' || image == null){
        //TODO
      warn("<<Info about image subscription will be placed here shortly>>\n")
    }


      trainingSchedulingEvents.push(currentScheduleEventDef)
  }
  
  return trainingSchedulingEvents
}

function isDict(x) {
  return x.constructor === Object
}

function get(object, key, default_value) {
  var result = object[key]
  return (typeof result !== "undefined") ? result : default_value
}


function fatal(msg) {
  console.error(`[FATAL] ${msg}`)
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

  try {

    volumeSizeValue = Math.ceil(parseFloat(volumeSizeArray[0]))
    if (parseFloat(volumeSizeArray[0]) != volumeSizeValue) {
      console.warn(
        `[WARNING] Provided 'volume_size': ${volumeSizeArray[0]} is not an integer but it should be -> rounding up to closest integer: ${volumeSizeValue}`)
    }

  }
  catch (error) {
    throw 'Unable to parse provided value of volume_size'
  }

  assert(volumeSizeValue > 0, 'volume_size must be > 0')
  // returning volume size GBs
  return volumeSizeValue
}

// execute configuration and set module.exports.trainingSchedulingEvents
configure()
