const assert = require('assert')
const fs = require('fs');

const empty_models_error =
    '\n###################################################################\n' +
    '### No model specified in `models` section of config/config.yml ###\n' +
    '###################################################################\n';

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


const MAX_RUNTIME_UNITS_TO_SECONDS = {seconds: 1, minutes: 60, hours: 3600, days: 86400};

const MAX_RUNTIME_NULL_ERROR =
    '\n###########################################################\n' +
    '###            max_runtime must not be empty            ###\n' +
    '###########################################################\n';

const MAX_RUNTIME_NOT_POSITIVE =
    '\n###########################################################\n' +
    '###               max_runtime must be > 0               ###\n' +
    '###########################################################\n';

const BAD_UNIT_ERROR =
    '\n###########################################################\n' +
    '###         Provided time unit is not supported         ###\n' +
    '###########################################################\n';

function parseMaxRuntimeString(maxRuntimeString){
    assert(!(maxRuntimeString == null), MAX_RUNTIME_NULL_ERROR);
    assert(!(maxRuntimeString == ''), MAX_RUNTIME_NULL_ERROR);
    // replace multiple spaces, tabs, etc with single space
    var maxRuntimeStringFixed = maxRuntimeString.replace(/\s\s+/g, ' ').trim();
    // split on space to separate value and unit
    var maxRuntimeArray = maxRuntimeStringFixed.split(' ');
    var maxRuntimeUnit = maxRuntimeArray[1].toLowerCase();

    assert(Object.keys(MAX_RUNTIME_UNITS_TO_SECONDS).includes(maxRuntimeUnit), BAD_UNIT_ERROR);

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


const MAX_DECISION_RECORDS_NEGATIVE_OR_NULL =
    '\n###########################################################\n' +
    '###    max_decision_records must be > 0 and not null    ###\n' +
    '###########################################################\n';

function parseMaxDecisionRecords(maxDecisionRecords){
    assert(!(maxDecisionRecords == null), MAX_DECISION_RECORDS_NEGATIVE_OR_NULL);
    assert(maxDecisionRecords  > 0, MAX_DECISION_RECORDS_NEGATIVE_OR_NULL);
    return maxDecisionRecords
}


function setTrainSchedulingEvents(scheduleEventPattern){
  //defaults
  var defaultScheduleString = module.exports.config['training']['schedule'];
  var defaultWorkerInstanceType = module.exports.config['training']['worker_instance_type'];
  var defaultWorkerCount = module.exports.config['training']['worker_count'];
  var defaultMaxRecordsPerWorker = module.exports.config['training']['max_records_per_worker'];
  var defaultMaxRuntimeInSeconds = module.exports.config['training']['max_runtime'];
  var defaultVolumeSizeInGB = module.exports.config['training']['volume_size_in_gb'];
  var defaultMaxRowsCount = module.exports.config['training']['max_decision_records'];

  var currentScheduleEventDef = null;
  var currentModelTrainingConfig = {};


  for (const [modelName, modelConfig] of Object.entries(module.exports.config['models'])) {

      if (modelConfig == null) {
          currentModelTrainingConfig = {};
      } else {
          currentModelTrainingConfig = get(modelConfig, 'training', {});
      }

      // deep copy dict
      currentScheduleEventDef = JSON.parse(JSON.stringify(scheduleEventPattern))
      // pass scheduling info
      currentScheduleEventDef['schedule']['rate'] =
          get(currentModelTrainingConfig, 'schedule', defaultScheduleString);

      // pass description
      currentScheduleEventDef['schedule']['description'] =
          `${currentScheduleEventDef['schedule']['rate']} schedule of ${modelName} model`;

      currentScheduleEventDef['schedule']['input'] = {};

      // pass env vars as parameters
      currentScheduleEventDef['schedule']['input']['model_name'] = modelName;
      currentScheduleEventDef['schedule']['input']['worker_instance_type'] =
          get(currentModelTrainingConfig, 'worker_instance_type', defaultWorkerInstanceType);
      currentScheduleEventDef['schedule']['input']['worker_count'] =
          get(currentModelTrainingConfig, 'worker_count', defaultWorkerCount);
      currentScheduleEventDef['schedule']['input']['max_records_per_worker'] =
          get(currentModelTrainingConfig, 'max_records_per_worker', defaultMaxRecordsPerWorker);
      currentScheduleEventDef['schedule']['input']['max_runtime'] =
          parseMaxRuntimeString(get(currentModelTrainingConfig, 'max_runtime', defaultMaxRuntimeInSeconds));
      currentScheduleEventDef['schedule']['input']['volume_size_in_gb'] =
          get(currentModelTrainingConfig, 'volume_size_in_gb', defaultVolumeSizeInGB);
      currentScheduleEventDef['schedule']['input']['max_decision_records'] =
            parseMaxDecisionRecords(get(currentModelTrainingConfig, 'max_decision_records', defaultMaxRowsCount));


      module.exports.trainSchedulingEvents.push(currentScheduleEventDef)
  }
}

const orgAndProjNameRegex = '^[a-z0-9]+$'
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
      "\n[WARNING] Please change 'organization' in config/config.yml - " +
      'currently detected example organization => `acme`\n')
}

if(image == '' || image == null){
  console.warn(
      "\n[WARNING] <<Info about image subscription will be placed here shortly>>\n");
}


assert(organization.match(orgAndProjNameRegex), 'config/config.yml:organization may contain only lowercase letters and numbers');
assert(project.match(orgAndProjNameRegex), 'config/config.yml:project may contain only lowercase letters and numbers');

assert(organization != '', 'config/config.yml:organization is an empty string');
assert(project != '', 'config/config.yml:project is an empty string');

assert(module.exports.config['models'] != null, empty_models_error);
// model names should be validated according to model naming rules
for (const [key, value] of Object.entries(module.exports.config['models'])) {
  assert(key.match(modelNameRegex), `Invalid model name: ${key}`)
}


module.exports.trainSchedulingEvents = [];
setTrainSchedulingEvents(scheduleEventPattern)
