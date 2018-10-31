'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const es = require('event-stream');
const uuidv4 = require('uuid/v4');
const dateFormat = require('date-format');
const s3 = new AWS.S3();

module.exports.unpackFirehose = function(event, context, cb) {

  console.log(`processing event records from firehose bucket SNS event ${JSON.stringify(event)}`)
  
  let now = new Date()
  let pathDatePart = dateFormat.asString("yyyy/MM/dd/hh", now)
  let filenameDatePart = dateFormat.asString("yyyy-MM-dd-hh-mm-ss",now)
  let uuidPart = uuidv4() // use the same uuid across the files for this unpacking
  
  let buffersByKey = {}
  let promises = []
  
  for (let i = 0 ; i < event.Records.length ; i++) {
    if (!event.Records[i].Sns || !event.Records[i].Sns.Message) {
        console.log(`WARN: Invalid S3 SNS notification ${JSON.stringify(event)}`)
        continue;
    }
    let snsMessage = JSON.parse(event.Records[i].Sns.Message)
    for (let j = 0 ; j < snsMessage.Records.length ; j++) {

        promises.push(new Promise((resolve, reject) => {
            
            let s3Record = snsMessage.Records[j].s3
    
            let gunzip = zlib.createGunzip()
            
            let stream = s3.getObject({
                Bucket: s3Record.bucket.name,
                Key: s3Record.object.key,
            }).createReadStream()
            .pipe(gunzip)
            .pipe(es.split()) // parse line-by-line
            .pipe(es.mapSync(function(line){
        
                // pause the readstream
                stream.pause();

                try {
                    if (!line) {
                        return;
                    }
                    let requestRecord = JSON.parse(line)

                    if (!requestRecord || !requestRecord.project_name) {
                        console.log(`WARN: no project_name for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    let projectName = requestRecord.project_name;
                    
                    // delete project_name from requestRecord in case its sensitive
                    delete requestRecord.project_name;
                    
                    let recordType = requestRecord.record_type;
                    
                    if (!recordType) {
                        console.log(`WARN: no record_type for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    if (!(recordType === "choose" || recordType === "using" || recordType === "rewards")) {
                        console.log(`WARN: invalid record_type for requestRecord ${requestRecord}`)
                        return;
                    }
                    
                    let model = null; // leave as null in case its a rewards record
                    
                    if (recordType === "choose" || recordType === "using") {
                      model = requestRecord.model;
                      if (!model) {
                        console.log(`WARN: no model for requestRecord ${requestRecord}`)
                        return;
                      }
                    }
                    
                    // TODO double check projectName, model valid chars
                    
                    // projectName/recordType/(model/)yyyy/MM/dd/hh/projectName-recordType-(model-)yyyy-MM-dd-hh-mm-ss-uuid.gz
                    let s3Key = module.exports.getS3KeyPrefix(recordType, projectName, model)+pathDatePart+"/"+
                      `improve-v3-${projectName}-${recordType}-`+(model ? `${model}-` : "")+filenameDatePart+"-"+uuidPart+".gz"
                    
                    let buffers = buffersByKey[s3Key]
                    if (!buffers) {
                      buffers = []
                      buffersByKey[s3Key] = buffers
                    }
                    buffers.push(Buffer.from(JSON.stringify(requestRecord)+"\n"))

                } catch (err) {
                    console.log(`error ${err} skipping requestRecord`)
                } finally {
                    stream.resume();
                }
            })
            .on('error', function(err){
                console.log('Error while reading file.', err);
                return reject(err)
            })
            .on('end', function(){
                return resolve()
            }));
        }))
    }
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
    return cb(null,'success')
  }, err => {
    return cb(err)
  })
}

module.exports.getS3KeyPrefix = (recordType, projectName, model) => {
  return `${projectName}/${recordType}/`+(model ? `${model}/` : "")
}
