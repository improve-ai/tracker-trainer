'use strict';

const s3utils = require("./s3utils.js")

const AWS = require('aws-sdk');
const fs = require('fs').promises;
const shajs = require('sha.js');
const pLimit = require('p-limit');
const uuidv4 = require('uuid/v4');

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

    const historyId = record.history_id
    
    if (!historyId || typeof historyId !== "string") {
      console.log(`WARN: skipping record - invalid history_id in ${JSON.stringify(record)}`)
      return;
    }

    let buffers = buffersByHistoryId[historyId];

    if (!buffers) {
      buffers = [];
      buffersByHistoryId[historyId] = buffers;
    }

    buffers.push(Buffer.from(JSON.stringify(record) + "\n"));
  }).then(() => {
    return writeRecords(buffersByHistoryId);
  })
}

function writeRecords(buffersByHistoryId) {
  const promises = [];
  let bufferCount = 0;
  const limit = pLimit(50); // limit concurrent writes
  
  // write out histories
  for (const [historyId, buffers] of Object.entries(buffersByHistoryId)) {

    bufferCount += buffers.length
    
    const fileName = uniqueFileName(historyId)

    // the file path at which the history data will be stored in the file storage
    const directoryBasePath = directoryPathForHistoryFileName(fileName)
    const path = `${directoryBasePath}${fileName}`;

    // write a new unique file.  It will be consolidated later into a single file per history during reward assignment
    promises.push(limit(() => fs.writeFile(path, Buffer.concat(buffers)).catch(err => {
      if (err && err.code === 'ENOENT') {
        // the parent dir probably doesn't exist, create it
        return fs.mkdir(directoryBasePath, { recursive: true }).catch(err => { 
          // mkdir may throw an EEXIST, swallow it
          if (err.code != 'EEXIST') throw err;
        }).then(() => {
          // try the write again
          fs.writeFile(path, Buffer.concat(buffers))
        })
      } else {
        throw err
      }
    })));
  }
  
  console.log(`writing ${bufferCount} records for ${Object.keys(buffersByHistoryId).length} history ids`)

  return Promise.all(promises);
}

function hashHistoryId(historyId) {
  return shajs('sha256').update(historyId).digest('hex')
}

function uniqueFileName(historyId) {
  return `${hashHistoryId(historyId)}-${uuidv4()}.jsonl.gz`
}

function directoryPathForHistoryFileName(fileName) {
  if (fileName.length != 110) {
    throw Error (`${fileName} isn't exactly 110 characters in length`)
  }
  return `${process.env.EFS_FILE_PATH}/histories/${fileName.substr(0,2)}/${fileName.substr(2,2)}/`;
}