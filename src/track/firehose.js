'use strict';

const AWS = require('aws-sdk');
const uuidv4 = require('uuid/v4');
const fs = require('fs').promises; // use this for parallel creation of the files using a promise array and resolving them all parallel fashion
const filesystem = require('fs');
const firehose = new AWS.Firehose();
const customize = require("./customize.js");
const shajs = require('sha.js');
const shell = require('shelljs');

const s3utils = require("./s3utils.js")

// Send the event with the timestamp and project name to firehose
module.exports.sendToFirehose = (projectName, body, receivedAt, log) => {
  body["project_name"] = projectName;
  body["received_at"] = receivedAt.toISOString();
  // FIX timestamp must never be in the future
  if (!body.timestamp) {
    body["timestamp"] = body["received_at"];
  }
  if (!body.message_id) {
    body["message_id"] = uuidv4()
  }
  let firehoseData = Buffer.from(JSON.stringify(body)+'\n')
  consoleTime('firehose',log)
  consoleTime('firehose-create',log)

  let firehosePromise = firehose.putRecord({
    DeliveryStreamName: process.env.FIREHOSE_DELIVERY_STREAM_NAME,
    Record: { 
        Data: firehoseData
    }
  }).promise().then(result => {
    consoleTimeEnd('firehose',log)
    return result
  })
  consoleTimeEnd('firehose-create',log)
  return firehosePromise;
}

module.exports.unpackFirehose = async function(event, context) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  if (!event.Records || !event.Records.length > 0 || event.Records.some(record => !record.s3 || !record.s3.bucket || !record.s3.bucket.name || !record.s3.object || !record.s3.object.key)) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }
  
  return Promise.all(event.Records.map(s3EventRecord => processFirehoseFile(s3EventRecord.s3.bucket.name, s3EventRecord.s3.object.key))).then((res) => {
    return res;
  }).catch(err => console.log(err));
}
 
function processFirehoseFile(s3Bucket, firehoseS3Key) {

  let buffersByFileName = {}

  return s3utils.processCompressedJsonLines(s3Bucket, firehoseS3Key, record => {

    try {
      record.project_name = customize.migrateProjectName(record.project_name || record.api_key) // api_key is deprecated
    } catch (err) {
      console.log(`WARN: skipping record ${JSON.stringify(record)}`, err)
      return;
    }

    // migrate improve v4 records
    if (record.record_type) {
      switch (record.record_type) {
        case "choose":
          // skip old choose records
          return;
        case "using":
          record.type = "decision"
          record.variant = record.properties
          delete record.properties
          break;
        case "rewards":
          record.type = "rewards"
          break;
      }
      delete record.record_type
    }

    if (!record.history_id) {
      record.history_id = record.user_id
      delete record.user_id
    }

    if (!record.project_name) {
      console.log(`WARN: skipping record - no project_name in ${JSON.stringify(record)}`)
      return;
    }

    if (!record.timestamp || !isValidDate(record.timestamp)) {
      console.log(`WARN: skipping record - invalid timestamp in ${JSON.stringify(record)}`)
      return;
    }

    const timestamp = new Date(record.timestamp)
    // client reporting of timestamps in the future are handled in sendToFireHose. This should only happen with some clock skew.
    if (timestamp > Date.now()) {
      console.log(`WARN: timestamp in the future ${JSON.stringify(record)}`)
    }
    
    // ensure everything in history is UTC
    record.timestamp = timestamp.toISOString()

    const projectName = record.project_name;

    
    // delete project_name from record in case it is sensitive
    delete record.project_name;
    
    if (!isValidProjectName(projectName)) {
      console.log(`WARN: skipping record - invalid project_name, not alphanumeric, underscore, dash, space, period ${JSON.stringify(record)}`)
      return;
    }

    if (!record.history_id) {
      console.log(`WARN: skipping record - no history_id in ${JSON.stringify(record)}`)
      return;
    }

    const fileName = shajs('sha256').update(record.history_id).digest('base64').replace(/[\W_]+/g,'').substring(0,24); //create the filename

    let buffers = buffersByFileName[fileName]; //make an array with filename as the key and the buffer value as the value-object

    if (!buffers) {
      buffers = [];
      buffersByFileName[fileName] = buffers;
    }

    buffers.push(Buffer.from(JSON.stringify(record) + "\n"));
  }).then(() => {
    return writeRecords(buffersByFileName);
  })
}

function writeRecords(buffersByFileName) {
  const promises = [];
  const directoryPathArr = [];
    
  // write out histories
  for (const [fileName, buffers] of Object.entries(buffersByFileName)) {

      // the file path at which the history data will be stored in the file storage
      const directoryBasePath = `${process.env.EFS_FILE_PATH}/histories/${fileName.substr(0,1)}/${fileName.substr(1,1)}/${fileName.substr(2,1)}/`;
      const path = `${directoryBasePath}${fileName}.jsonl`;

      if (!filesystem.existsSync(directoryBasePath)) {
        shell.mkdir('-p', directoryBasePath); // create a directory if a folder path with their sub-folders doesn't exist
        directoryPathArr.push(directoryBasePath);
      }

      promises.push(fs.writeFile(path, Buffer.concat(buffers)));
    }

  console.log(`The value of the directory path is : ${JSON.stringify(directoryPathArr)} and in total ${directoryPathArr.length} directories will be created as it doesn't exist in EFS`);
  
  return Promise.all(promises);
}

function consoleTime(name, shouldLog) {
  if (shouldLog) {
    console.time(name);
  }
}

function consoleTimeEnd(name, shouldLog) {
  if (shouldLog) {
    console.timeEnd(name);
  }
}


// from https://stackoverflow.com/questions/7445328/check-if-a-string-is-a-date-value
function isValidDate(date) {
  return !!parseDate(date)
}

function parseDate(dateString) {
  const date = new Date(dateString)
  if ((date !== "Invalid Date") && !isNaN(date)) {
    return date
  } else {
    return null
  }
}

// allow alphanumeric, underscore, dash, space, period
function isValidModelName(modelName) {
  return modelName.match(/^[\w\- .]+$/i)
}

function isValidProjectName(projectName) {
  return isValidModelName(projectName) // same rules
}
