'use strict';

const s3utils = require("./s3utils.js")

const AWS = require('aws-sdk')
const fs = require('fs').promises
const shajs = require('sha.js')
const pLimit = require('p-limit')
const uuidv4 = require('uuid/v4')
const zlib = require('zlib')

/*
When a new file is written to the Firehose S3 bucket, unpack the file and write out seperate files for each history_id to the EFS 'incoming' directory.
*/
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
  let skippedRecordCount = 0

  return s3utils.processCompressedJsonLines(s3Bucket, firehoseS3Key, record => {

    const historyId = record.history_id
    
    if (!historyId || typeof historyId !== "string") {
      skippedRecordCount++
      return;
    }

    let buffers = buffersByHistoryId[historyId];

    if (!buffers) {
      buffers = [];
      buffersByHistoryId[historyId] = buffers;
    }

    buffers.push(Buffer.from(JSON.stringify(record) + "\n"));
  }).then(() => {
    if (skippedRecordCount) {
      console.log(`skipped ${skippedRecordCount} records due to invalid history_id`)
    }
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
    
    // create a new unique file.  It will be consolidated later into a single file per history during reward assignment
    const fileName = uniqueFileName(historyId)

    const fullPath = `${process.env.INCOMING_FILE_PATH}/${fileName}`

    const compressedData = zlib.gzipSync(Buffer.concat(buffers))

    promises.push(limit(() => fs.writeFile(fullPath, compressedData)));
  }
  
  console.log(`writing ${bufferCount} records for ${Object.keys(buffersByHistoryId).length} history ids`)

  return Promise.all(promises)
}

function uniqueFileName(historyId) {
  // TODO unit test
  return `${shajs('sha256').update(historyId).digest('hex')}-${uuidv4()}.jsonl.gz`
}
