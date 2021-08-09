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
proejct = module.exports.config['project'];

assert(!(organization == null), 'Organization name is null or undefined');
assert(!(proejct == null), 'Project name is null or undefined');

if(organization == 'acme'){
  console.warn(
      '\n[WARNING] Please change the organization in the config.yml - ' +
      'currently detected default organization => `acme`\n')
}

assert(organization.match(orgAndProjNameRegex), 'Organization name contains illegal characters');
assert(proejct.match(orgAndProjNameRegex), 'Project name contains illegal characters');

assert(organization != '', 'Organization name is an empty string');
assert(proejct != '', 'Project name is an empty string');

// model names should be validated according to model naming rules
for (const [key, value] of Object.entries(module.exports.config['models'])) {
  assert(key.match(modelNameRegex), `Invalid model name: ${key}`)
}
