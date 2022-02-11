const assert = require('assert');
const fs = require('fs');

let yaml = null;

try {
    yaml = require('yaml');
} catch(error) {
    console.log(
        '#############################################################\n' +
        '### Please run `npm install` before deploying application ###\n' +
        '#############################################################\n');
    throw(error);
}


function get(object, key, default_value) {
    var result = object[key];
    return (typeof result !== "undefined") ? result : default_value;
}



function checkFixAndSplitValueWithUnit(checkedString, emptyOrNullStringError, checkedParameterName){
    assert(!(checkedString == null), emptyOrNullStringError);
    assert(!(checkedString == ''), emptyOrNullStringError);
    // replace multiple spaces, tabs, etc with single space
    var checkedStringFixed = checkedString.toString().replace(/\s\s+/g, ' ').trim();
    // split on space to separate value and unit
    var checkedStringArray = checkedStringFixed.split(' ');

    var bad_length_message =
        '\n###########################################################\n' +
           `###              ${checkedParameterName} has bad format             ###\n` +
           '###########################################################\n';

    assert(checkedStringArray.length == 2, bad_length_message)
    return checkedStringArray
}

const MAX_RUNTIME_UNITS_TO_SECONDS = {seconds: 1, minutes: 60, hours: 3600, days: 86400};

const MAX_RUNTIME_NULL_ERROR =
    '\n###########################################################\n' +
    '###            max_runtime must not be empty            ###\n' +
    '###########################################################\n';

const MAX_RUNTIME_NOT_POSITIVE =
    '\n###########################################################\n' +
    '###               max_runtime must be > 0               ###\n' +
    '###########################################################\n';

const BAD_TIME_UNIT_ERROR =
    '\n###########################################################\n' +
    '###         Provided time unit is not supported         ###\n' +
    '###########################################################\n';

function parseMaxRuntimeString(maxRuntimeString){

    var maxRuntimeParameterName = 'max_runtime';
    var maxRuntimeArray = checkFixAndSplitValueWithUnit(
        maxRuntimeString, MAX_RUNTIME_NULL_ERROR, maxRuntimeParameterName);
    var maxRuntimeUnit = maxRuntimeArray[1].toLowerCase();

    assert(Object.keys(MAX_RUNTIME_UNITS_TO_SECONDS).includes(maxRuntimeUnit), BAD_TIME_UNIT_ERROR);

    var maxRuntimeValue = -1;

    try {
        maxRuntimeValue = parseInt(maxRuntimeArray[0]);
    } catch(error) {
        throw '\n#####################################################\n' +
                '### Unable to parse provided value of max runtime ###\n' +
                '#####################################################\n';
    }

    assert(maxRuntimeValue > 0, MAX_RUNTIME_NOT_POSITIVE)
    return maxRuntimeValue * MAX_RUNTIME_UNITS_TO_SECONDS[maxRuntimeUnit]
}


const VOLUME_SIZE_NULL_ERROR =
    '\n###########################################################\n' +
      '###            volume_size must not be empty            ###\n' +
      '###########################################################\n';


const VOLUME_SIZE_NEGATIVE_ERROR =
    '\n###########################################################\n' +
      '###               volume_size must be > 0               ###\n' +
      '###########################################################\n';


const BAD_VOLUME_UNIT_ERROR =
    '\n###########################################################\n' +
      '###      Provided volume unit is not supported          ###\n' +
      '###########################################################\n';


function parseVolumeSize(volumeSizeString){

    var volumeSizeParameterName = 'volume_size';
    var volumeSizeArray = checkFixAndSplitValueWithUnit(
        volumeSizeString, VOLUME_SIZE_NULL_ERROR, volumeSizeParameterName);
    var volumeSizeUnit = volumeSizeArray[1];

    assert(volumeSizeUnit == 'GB', BAD_VOLUME_UNIT_ERROR);

    var volumeSizeValue = -1;

    try {

        volumeSizeValue  = Math.ceil(parseFloat(volumeSizeArray[0]));
        if (parseFloat(volumeSizeArray[0]) != volumeSizeValue){
            console.warn(
                `[WARNING] Provided 'volume_size': ${volumeSizeArray[0]} is not an integer but it should be -> rounding up to closest integer: ${volumeSizeValue}\n`);
        }

    } catch(error) {
        throw '\n#####################################################\n' +
                '### Unable to parse provided value of volume size ###\n' +
                '#####################################################\n';
    }

    assert(volumeSizeValue > 0, VOLUME_SIZE_NEGATIVE_ERROR)
    // returning volume size GBs
    return volumeSizeValue


}


const MAX_DECISION_RECORDS_NEGATIVE_OR_NULL =
    '\n###########################################################\n' +
    '###    max_decision_records must be > 0 and not null    ###\n' +
    '###########################################################\n';


function parseMaxDecisionRecords(maxDecisionRecords){
    assert(!(maxDecisionRecords == null), MAX_DECISION_RECORDS_NEGATIVE_OR_NULL);
    assert(maxDecisionRecords  > 0, MAX_DECISION_RECORDS_NEGATIVE_OR_NULL);
    return maxDecisionRecords
}


function parseHyperparameters(hyperparameters){
    for (const [parameterName, parameterValue] of Object.entries(hyperparameters)){
        if(parameterName == 'max_decision_records'){
            hyperparameters[parameterName] = parseMaxDecisionRecords(parameterValue);
        }
    }
    return hyperparameters

}

function setTrainSchedulingEvents(scheduleEventPattern){
  console.log(`[INFO] Loading default training config\n`)
    //defaults
  var defaultScheduleString = module.exports.config['training']['schedule'];
  var defaultWorkerInstanceType = module.exports.config['training']['instance_type'];
  var defaultWorkerCount = module.exports.config['training']['instance_count'];
  var defaultMaxRuntimeInSeconds = module.exports.config['training']['max_runtime'];
  var defaultVolumeSize = module.exports.config['training']['volume_size'];
  var defaultHyperparameters = module.exports.config['training']['hyperparameters'];

  var currentScheduleEventDef = null;
  var currentModelTrainingConfig = {};


  for (const [modelName, modelConfig] of Object.entries(module.exports.config['models'])) {

      console.log(`[INFO] Processing training configuration for model: ${modelName}\n`)
      if (modelConfig == null) {
          currentModelTrainingConfig = {};
      } else {
          currentModelTrainingConfig = get(modelConfig, 'training', {});
          if(currentModelTrainingConfig === {}){
              console.warn(`[WARNING] No 'training' section found in model config for model: ${modelName}\n`)
          }
      }

      // deep copy dict
      currentScheduleEventDef = JSON.parse(JSON.stringify(scheduleEventPattern))
      // set rule name
      currentScheduleEventDef['schedule']['name'] =
          `improveai-${module.exports.config['organization']}-${module.exports.config['project']}-` + '${opt:stage, self:provider.stage}' + `-${modelName}-schedule`;
      // pass scheduling info
      currentScheduleEventDef['schedule']['rate'] =
          get(currentModelTrainingConfig, 'schedule', defaultScheduleString);

      // pass description
      currentScheduleEventDef['schedule']['description'] =
          `${currentScheduleEventDef['schedule']['rate']} schedule of ${modelName} model`;

      currentScheduleEventDef['schedule']['input'] = {};

      // pass env vars as parameters
      currentScheduleEventDef['schedule']['input']['model_name'] = modelName;
      currentScheduleEventDef['schedule']['input']['instance_type'] =
          get(currentModelTrainingConfig, 'instance_type', defaultWorkerInstanceType);
      currentScheduleEventDef['schedule']['input']['instance_count'] =
          get(currentModelTrainingConfig, 'instance_count', defaultWorkerCount);
      currentScheduleEventDef['schedule']['input']['max_runtime'] =
          parseMaxRuntimeString(get(currentModelTrainingConfig, 'max_runtime', defaultMaxRuntimeInSeconds));
      currentScheduleEventDef['schedule']['input']['volume_size'] =
          parseVolumeSize(get(currentModelTrainingConfig, 'volume_size', defaultVolumeSize));
      currentScheduleEventDef['schedule']['input']['hyperparameters'] =
            parseHyperparameters(get(currentModelTrainingConfig, 'hyperparameters', defaultHyperparameters));

      module.exports.trainSchedulingEvents.push(currentScheduleEventDef)

  }
}

// Unit-tested in tests/test_regexps.py:test_predeploy_org_and_project_name_regexp()
const orgAndProjNameRegex = '^[a-z0-9]+$'
// Unit-tested in tests/test_regexps.py:test_model_name_regexp()
const modelNameRegex = /^[a-zA-Z0-9][\w\-.]{0,63}$/i
const config_file = fs.readFileSync('./config/config.yml', 'utf8');
// Apply defaults to this pattern
const scheduleEventPattern = {
    "schedule": {
        "name": null,
        "description": "default schedule",
        "rate": null,
        "enabled": true,
        "input": null
    }
};

module.exports.config = yaml.parse(config_file);
module.exports.config_json = JSON.stringify(module.exports.config);

// assert organization and project may contain only lowercase letters and
// numbers and must be non-null, non-empty strings
organization = module.exports.config['organization'];
project = module.exports.config['project'];
image = module.exports.config['training']['image']

assert(!(organization == null), 'config/config.yml:organization is null or undefined');
assert(!(project == null), 'config/config.yml:project is null or undefined');

if(organization == 'acme'){
  console.warn(
      "[WARNING] Please change 'organization' in config/config.yml - " +
      'currently detected example organization => `acme`\n')
}

if(image == '' || image == null){
  console.warn(
      "\n[WARNING] <<Info about image subscription will be placed here shortly>>\n");
}

function isDict(x) {
    return x.constructor === Object;
  }

assert(organization.match(orgAndProjNameRegex), 'config/config.yml:organization may contain only lowercase letters and numbers');
assert(project.match(orgAndProjNameRegex), 'config/config.yml:project may contain only lowercase letters and numbers');

assert(organization != '', 'config/config.yml:organization is an empty string');
assert(project != '', 'config/config.yml:project is an empty string');

module.exports.trainSchedulingEvents = [];
if (module.exports.config['models'] === null) {

    console.warn("[WARNING] No models were found, no models will be trained.");

} else {

    assert(isDict(module.exports.config['models']), "models' entries should be dictionaries");

    // model names should be validated according to model naming rules
    for (const [key, value] of Object.entries(module.exports.config['models'])) {
      assert(key.match(modelNameRegex), `Invalid model name: ${key}`)
    }

    setTrainSchedulingEvents(scheduleEventPattern)
}