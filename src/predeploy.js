const assert = require('assert')
const fs = require('fs');
const yaml = require('yaml');
//const yaml = require('js-yaml');

const orgAndProjNameRegex = '^[a-z0-9]+$'
const modelNameRegex = /^[\w\- .]+$/i

const config_file = fs.readFileSync('./config/config.yml', 'utf8');
module.exports.config = yaml.parse(config_file);
// module.exports.config = yaml.safeLoad(fs.readFileSync('./config/config.yml', 'utf8'));

module.exports.config_json = JSON.stringify(module.exports.config);

// assert organization and project may contain only lowercase letters and
// numbers and must be non-null, non-empty strings
organization = module.exports.config['organization'];
project = module.exports.config['project'];

assert(!(organization == null), 'config/config.yml:organization is null or undefined');
assert(!(project == null), 'config/config.yml:project is null or undefined');

if(organization == 'acme'){
  console.warn(
      "\n[WARNING] Please change 'organization' in config/config.yml - " +
      'currently detected example organization => `acme`\n')
}

assert(organization.match(orgAndProjNameRegex), 'config/config.yml:organization may contain only lowercase letters and numbers');
assert(project.match(orgAndProjNameRegex), 'config/config.yml:project may contain only lowercase letters and numbers');

assert(organization != '', 'config/config.yml:organization is an empty string');
assert(project != '', 'config/config.yml:project is an empty string');

// model names should be validated according to model naming rules
for (const [key, value] of Object.entries(module.exports.config['models'])) {
  assert(key.match(modelNameRegex), `Invalid model name: ${key}`)
}
