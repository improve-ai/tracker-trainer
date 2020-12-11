'use strict';

const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const zlib = require('zlib')
const es = require('event-stream')

module.exports.processCompressedJsonLines = (s3BucketName, s3BucketKey, mapFunction) => {   

    let results = [];

    return new Promise((resolve, reject) => {

        console.log(`loading ${s3BucketName} from ${s3BucketKey}`) 

        var params = {
            Bucket: s3BucketName, // the s3 bucket name,
            Key: s3BucketKey // path to the object which was recently added
        }
        
        var gunZip = zlib.createGunzip();
        
        let stream = s3.getObject(params).createReadStream().pipe(gunZip).pipe(es.split()).pipe(es.mapSync((line) => {
 
            try{
            stream.pause();
        
            let record;
        
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
            return resolve(results)
        })
        )
    }) 
}