'use strict';

const AWS = require('aws-sdk')
const s3 = new AWS.S3({maxRetries: 10})
const zlib = require('zlib')
const es = require('event-stream')

// get object stream, unpack json lines and process each json object one by one using mapFunction
module.exports.processCompressedJsonLines = (s3Bucket, s3Key, mapFunction) => {
  let results = []
  
  return new Promise((resolve, reject) => {
    
    let gunzip = zlib.createGunzip()

    console.log(`loading ${s3Key} from ${s3Bucket}`)

    let stream = s3.getObject({ Bucket: s3Bucket, Key: s3Key,}).createReadStream().pipe(gunzip).pipe(es.split()).pipe(es.mapSync(function(line) {

      // pause the readstream
      stream.pause();

      let record
      try {
        if (!line) {
          return;
        }

        try {
          record = JSON.parse(line)
        } catch (err) {
          console.log(`error ${err} skipping record ${line}`)
        }

        if (!record) {
          return;
        }
        const result = mapFunction(record)

        if (result) {
          results.push(result)
        }
      } finally {
        stream.resume();
      }
    })
    .on('error', function(err) {
      console.log('Error while reading file.', err);
      return reject(err)
    })
    .on('end', function() {
      return resolve(results)
    }));
  })
}
