const yaml = require('js-yaml');
const fs = require('fs');

module.exports.config = JSON.stringify(yaml.safeLoad(fs.readFileSync('./config/config.yml', 'utf8')));
