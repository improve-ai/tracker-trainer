'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const es = require('event-stream');
const uuidv4 = require('uuid/v4');
const shajs = require('sha.js')
const dateFormat = require('date-format');
const s3 = new AWS.S3();

module.exports.unpackFirehose = function(event, context, cb) {

  console.log(`processing event records from firehose bucket SNS event ${JSON.stringify(event)}`)

  let now = new Date()
  let filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss", now)
  let uuidPart = uuidv4() // use the same uuid across the files for this unpacking

  let buffersByKey = {}
  let promises = []

  if (!event.Records || !event.Records.length > 0) {
    return cb(new Error(`WARN: Invalid S3 event ${JSON.stringify(event)}`))
  }

  for (let i = 0; i < event.Records.length; i++) {
    if (!event.Records[i].s3) {
      console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`)
      continue;
    }

    promises.push(new Promise((resolve, reject) => {

      let s3Record = event.Records[i].s3

      let gunzip = zlib.createGunzip()

      let stream = s3.getObject({
          Bucket: s3Record.bucket.name,
          Key: s3Record.object.key,
        }).createReadStream()
        .pipe(gunzip)
        .pipe(es.split()) // parse line-by-line
        .pipe(es.mapSync(function(line) {

            // pause the readstream
            stream.pause();

            try {
              if (!line) {
                return;
              }
              let eventRecord = JSON.parse(line)

              if (!eventRecord || !eventRecord.project_name) {
                console.log(`WARN: skipping record - no project_name in ${line}`)
                return;
              }

              if (!eventRecord.user_id) {
                console.log(`WARN: skipping record - no user_id in ${line}`)
                return;
              }

              let projectName = eventRecord.project_name;

              // delete project_name from requestRecord in case its sensitive
              delete eventRecord.project_name;

              // allow alphanumeric, underscore, dash, space, period
              if (!projectName.match(/^[\w\- .]+$/i)) {
                console.log(`WARN: skipping record - invalid project_name, not alphanumeric, underscore, dash, space, period ${line}`)
                return;
              }

              let hashedUserId = getHashedUserId(projectName, eventRecord.user_id);

              // projectName/histories/hashedUserId/improve-v5-pending-events-projectName-hashedUserId-yyyy-MM-dd-hh-mm-ss-uuid.gz
              let s3Key = `${projectName}/histories/${hashedUserId}/improve-v5-pending-events-${projectName}-${hashedUserId}-${filenameDatePart}-${uuidPart}.gz`

              let buffers = buffersByKey[s3Key]
              if (!buffers) {
                buffers = []
                buffersByKey[s3Key] = buffers
              }
              buffers.push(Buffer.from(JSON.stringify(eventRecord) + "\n"))

            }
            catch (err) {
              console.log(`error ${err} skipping requestRecord`)
            }
            finally {
              stream.resume();
            }
          })
          .on('error', function(err) {
            console.log('Error while reading file.', err);
            return reject(err)
          })
          .on('end', function() {
            return resolve()
          }));
    }))
  }

  return Promise.all(promises).then(results => {
    // ignore results, just use the shared buffersByKey

    let promises = []

    for (var s3Key in buffersByKey) {
      if (!buffersByKey.hasOwnProperty(s3Key)) {
        continue
      }

      let buffers = buffersByKey[s3Key]
      if (!buffers.length) {
        continue;
      }

      console.log(`writing ${buffers.length} records to ${s3Key}`)

      let params = {
        Body: zlib.gzipSync(Buffer.concat(buffers)),
        Bucket: process.env.RECORDS_BUCKET,
        Key: s3Key
      }

      promises.push(s3.putObject(params).promise())
    }

    return Promise.all(promises)
  }).then(results => {
    return cb(null, 'success')
  }, err => {
    return cb(err)
  })
}

function getHashedUserId(projectName, userId) {
  return shajs('sha256').update(projectName.length + ":" + projectName + ":" + userId).digest('base64').replace(/[\W_]+/g, '').substring(0, 24); // remove all non-alphanumeric (+=/) then truncate to 144 bits
}
