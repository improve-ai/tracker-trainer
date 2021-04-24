'use strict';

const fs = require('fs').promises
const uuidv4 = require('uuid/v4')

/*
  When a new file is written to the Firehose S3 bucket, write a marker .json 
  file to the EFS firehose_incoming directory with a link to the S3 file.  The
  next time reward assignment is run it will download and ingest the S3 files.
*/
module.exports.markFirehoseIncoming = async function(event, context) {

  console.log(`processing s3 event ${JSON.stringify(event)}`)

  if (!event.Records || !event.Records.length > 0 || event.Records.some(record => !record.s3 || !record.s3.bucket || !record.s3.bucket.name || !record.s3.object || !record.s3.object.key)) {
    throw new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
  }
  
  return Promise.all(event.Records.map((s3EventRecord) => {

    const fullPath = `${process.env.FIREHOSE_INCOMING_FILE_PATH}/${uuidv4()}.json`
    
    const buffer = Buffer.from(JSON.stringify({ "s3_bucket": s3EventRecord.s3.bucket.name, "s3_key": s3EventRecord.s3.object.key }))
    
    console.log(`writing ${fullPath}`)
    
    return fs.writeFile(fullPath, buffer)

  }))
}