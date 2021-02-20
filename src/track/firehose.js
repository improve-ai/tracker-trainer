'use strict';

const customize = require("./customize.js");
const s3utils = require("./s3utils.js")

const AWS = require('aws-sdk');
const fs = require('fs').promises; // use this for parallel creation of the files using a promise array and resolving them all parallel fashion
const filesystem = require('fs');
const shajs = require('sha.js');
const pLimit = require('p-limit');

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

  let buffersByHistoryId = {}

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

    let buffers = buffersByHistoryId[record.history_id];

    if (!buffers) {
      buffers = [];
      buffersByHistoryId[record.history_id] = buffers;
    }

    buffers.push(Buffer.from(JSON.stringify(record) + "\n"));
  }).then(() => {
    return writeRecords(buffersByHistoryId);
  })
}

function writeRecords(buffersByHistoryId) {
  const promises = [];
  let bufferCount = 0;
  const limit = pLimit(500); // limit concurrent writes
  
  // write out histories
  for (const [historyId, buffers] of Object.entries(buffersByHistoryId)) {

    bufferCount += buffers.length
    
    const fileName = fileNameFromHistoryId(historyId)

    // the file path at which the history data will be stored in the file storage
    const directoryBasePath = directoryPathForHistoryFileName(fileName)
    const path = `${directoryBasePath}${fileName}`;

    // create a directory if a folder path with their sub-folders doesn't exist
    promises.push(limit(() => fs.access(directoryBasePath).catch(err => {
      if (err && err.code === 'ENOENT') {
        return fs.mkdir(directoryBasePath, { recursive: true })
      } else {
        throw err
      }
    }).catch(err => { 
      // mkdir may throw an EEXIST, swallow it
      if (err.code != 'EEXIST') throw err;
    }).then(() => {
      // There is intentionally no locking for performance. Firehose ingest is triggered every 15 minutes or when
      // the buffer reaches 128 MB so should run largely as one worker at a time. If two workers were to run simultaneously
      // they would have to write to the same file simultaneously to potentially cuase corruption.  Even then, using the JSON line format
      // in append only mode probably only a few individual records would be corrupted and the overall file should still be parseable.
      //
      // These assumptions are not true in the case of a bulk ingest, in which case you may limit the lambda concurrency to 1
      // or simply accept that some fraction of records will become corrupt. The data written to EFS is only used for model training
      // which is robust against a small portion of the data being missing or corrupt. The original master copy of the data will
      // still be available in S3.
      fs.appendFile(path, Buffer.concat(buffers))
    })));
  }
  
  console.log(`writing ${bufferCount} records for ${Object.keys(buffersByHistoryId).length} history ids`)

  return Promise.all(promises);
}

function fileNameFromHistoryId(historyId) {
  return `${shajs('sha256').update(historyId).digest('hex')}.jsonl`
}

function directoryPathForHistoryFileName(fileName) {
  if (fileName.length != 70) {
    throw Error (`${fileName} isn't exactly 70 characters in length`)
  }
  return `${process.env.EFS_FILE_PATH}/histories/${fileName.substr(0,2)}/${fileName.substr(2,2)}/`;
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
