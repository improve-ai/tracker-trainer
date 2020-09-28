'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const tar = require('tar-stream');
const zlib = require('zlib');
const stream = require('stream');
const uuidv4 = require('uuid/v4');

/*
 Expects s3:ObjectCreated event which is fired when SageMaker creates model artifcats.
 Expected S3 key should be like: 'transformed_models/<project_name>/<model_name>/test-mindful-messages10-202005112319-83072c36cecb-f/output/model.tar.gz.out'
 */
module.exports.unpack = function(event, context) {
  
  console.log(`processing event records from firehose bucket SNS event ${JSON.stringify(event)}`);

  /*
   We expect only 1 s3 event record, see forums:
   https://forums.aws.amazon.com/thread.jspa?messageID=592264#592264
   */
  if (!event.Records[0].s3) {
    console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`);
    context.fail();
    return;
  }
  
  let s3Record = event.Records[0].s3;
  const inputKey = s3Record.object.key;
  const pathParts = inputKey.split('/');
  if (pathParts[pathParts.length - 1] != 'model.tar.gz.out') {
    console.log(`WARN: Invalid S3 event ${JSON.stringify(event)}`);
    context.fail();
    return;
  }

  const [baseDir, projectName, modelName] = pathParts;

  var params = {
      Bucket: s3Record.bucket.name,
      Key: inputKey
  };

  var dataStream = s3.getObject(params).createReadStream();

  var extract = tar.extract();

  extract.on('entry', function(header, inputStream, next) {
      const key = getTimestampedS3KeyForFile(projectName, modelName, header.name);
      const latestKey = getLatestS3KeyForFile(projectName, modelName, header.name);
      
      if (key && latestKey) {
        inputStream.pipe(uploadFromStream(key, latestKey));
  
        inputStream.on('end', function() {
            next(); // ready for next entry
        });
      } else {
        next();
      }

      inputStream.resume(); // just auto drain the stream
  });

  extract.on('finish', function() {
      // all entries read
  });

  dataStream.pipe(zlib.createGunzip()).pipe(extract);
}

function uploadFromStream(key, latestKey) {
    var pass = new stream.PassThrough();

    var writeParams = {
        Bucket: process.env.MODELS_BUCKET,
        Key: key,
        Body: pass
    };
    
    if (key.endsWith(".mlmodel")) {
      writeParams.ContentType = "application/protobuf" // allows cloudfront to automatically compress .mlmodel files
    }

    const copyParams = {
      Bucket: process.env.MODELS_BUCKET,
      CopySource: process.env.MODELS_BUCKET + '/' + key,
      Key: latestKey
    };

    console.log(`uplading with params ${JSON.stringify(writeParams)}`)
    s3.upload(writeParams).promise().then((data) => {
      console.log(`copying with params ${JSON.stringify(copyParams)}`)
      return s3.copyObject(copyParams).promise();
    }).then((data) => {
      console.log('copy completed');
    }, (err) => {
      console.log('copy failed', err);
    });

    return pass;
}

function getTimestampedS3KeyForFile(projectName, modelName, filePath) {
  if (filePath.endsWith('.tar.gz')) {
    return getTimestampedXgbS3Key(projectName, modelName)
  } else if (filePath.endsWith('.mlmodel')) {
    return getTimestampedMLModelS3Key(projectName, modelName)
  } else {
    return null;
  }
}

function getLatestS3KeyForFile(projectName, modelName, filePath) {
  if (filePath.endsWith('.tar.gz')) {
    return getLatestXgbS3Key(projectName, modelName)
  } else if (filePath.endsWith('.mlmodel')) {
    return getLatestMLModelS3Key(projectName, modelName)
  } else {
    return null;
  }
}

function getLatestMLModelS3Key(projectName, modelName) {
  return `models/${projectName}/latest/improve-${modelName}.mlmodel`
}

function getLatestXgbS3Key(projectName, modelName) {
  return `models/${projectName}/latest/improve-${modelName}.tar.gz`
}

function getTimestampedMLModelS3Key(projectName, modelName) {
  return getTimestampedS3Key(projectName, modelName, "mlmodel");
}

function getTimestampedXgbS3Key(projectName, modelName) {
  return getTimestampedS3Key(projectName, modelName, "tar.gz");
}

function getTimestampedS3Key(projectName, modelName, extension) {
  const now = new Date();
  let dateStr = `${now.getUTCFullYear()}-${now.getUTCMonth()}-${now.getUTCDate()}-`;
  dateStr += `${now.getUTCHours()}-${now.getUTCMinutes()}-${now.getUTCSeconds()}`;

  const uuidStr = uuidv4();

  return `models/${projectName}/archive/improve-${modelName}-${dateStr}-${uuidStr}.${extension}`
}
