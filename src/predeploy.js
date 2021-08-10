const assert = require('assert')
const fs = require('fs');
const yaml = require('yaml');


function get(object, key, default_value) {
    var result = object[key];
    return (typeof result !== "undefined") ? result : default_value;
}


function setTrainSchedulingEvents(scheduleEventPattern){
  //defaults
  var defaultScheduleString = module.exports.config['training']['schedule'];
  var defaultWorkerInstanceType = module.exports.config['training']['worker_instance_type'];
  var defaultWorkerCount = module.exports.config['training']['worker_count'];
  var defaultMaxRecordsPerWorker = module.exports.config['training']['max_records_per_worker'];
  var defaultMaxRuntimeInSeconds = module.exports.config['training']['max_runtime_in_seconds'];

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
      // pass schedule name
      currentScheduleEventDef['schedule']['name'] = `${modelName}-schedule`;

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
      currentScheduleEventDef['schedule']['input']['max_runtime_in_seconds'] =
          get(currentModelTrainingConfig, 'max_runtime_in_seconds', defaultMaxRuntimeInSeconds);

      module.exports.trainSchedulingEvents.push(currentScheduleEventDef)
  }
}

const orgAndProjNameRegex = '^[a-z0-9]+$'
const modelNameRegex = /^[\w\- .]+$/i
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

// model names should be validated according to model naming rules
for (const [key, value] of Object.entries(module.exports.config['models'])) {
  assert(key.match(modelNameRegex), `Invalid model name: ${key}`)
}


module.exports.trainSchedulingEvents = [];
setTrainSchedulingEvents(scheduleEventPattern)
