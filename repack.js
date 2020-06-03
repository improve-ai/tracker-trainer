/*
 https://trello.com/c/FiU0gKYK/93-modify-repackjs-to-merge-multiple-models-into-a-single-latesttargz

 A node.js function that
 - is triggered by an SNS event when one of the models in the records bucket is modified
 - downloads archives (iOS & Android) which contain all models of the project
 - adds (overwriting) the modified model to both archives
 - two .tar.gz archives are created, they contain all models for the project
 - archives uploaded to the s3 models bucket.
 */

const AWS = require('aws-sdk');
const tar = require('tar');
const uuidv4 = require('uuid/v4');
const dateFormat = require('date-format');
const _ = require('lodash');
const stream = require('stream');
const naming = require('./naming.js');
const util = require('util');

const firehose = new AWS.Firehose();
const s3 = new AWS.S3();
const lambda = new AWS.Lambda();

/*
 Expects s3:ObjectCreated event which is fired when SageMaker creates model artifcats.
 Expected S3 key should be like: 'transformed_models/<project_name>/<model_name>/test-mindful-messages10-202005112319-83072c36cecb-f/output/model.tar.gz.out'
 */
module.exports.repack = async function(event, context) {

  //console.log(`processing event records from firehose bucket SNS event ${JSON.stringify(event)}`);

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
  pathParts = inputKey.split('/');
  if (pathParts[pathParts.length - 1] != 'model.tar.gz.out') {
    return;
  }

  const [baseDir, projectName, modelName] = pathParts;

  // Check if model should be added
  const modelsByProject = naming.getModelsByProject();
  const models = modelsByProject[projectName];
  if (models && !models.includes(modelName)) {
    context.fail();
    return; // Skip
  }

  // Prepare output streams
  let packiOS = new tar.Pack({
    gzip: true,
    portable: true,
    filter: (path) => path.endsWith('.mlmodel') || path.endsWith('.json')
  });
  let packAndroid = new tar.Pack({
    gzip: true,
    portable: true,
    filter: (path) => path.endsWith('.xgb') || path.endsWith('.json')
  });

  const multiarchiveiOSKey = 'models/' + projectName + '/mlmodel/latest.tar.gz';
  const multiarchiveAndroidKey = 'models/' + projectName + '/xgb/latest.tar.gz';
  const historyKeys = makeTimestampedKeys('models/' + projectName);
  const modelsBucket = process.env.MODELS_BUCKET;
  const pipeline = util.promisify(stream.pipeline);

  // Unpack existing models
  const modelNameFileFilter = (path) => {
    // Return true if the file is related to a model listed in customize.yml
    const modelName = path.split('.')[0];
    return modelName && models && !models.includes(modelName);
  };
  if (await isFieExists(modelsBucket, multiarchiveiOSKey)) {

    let iOSDownloadStream = s3.getObject({
      Bucket: modelsBucket,
      Key: multiarchiveiOSKey
    })
    .createReadStream();

    let parseiOS = new tar.Parse({ filter: modelNameFileFilter })
    .on('entry', function(entry) {
      packiOS.add(entry);
    });

    await pipeline(iOSDownloadStream, parseiOS);
  }

  if (await isFieExists(modelsBucket, multiarchiveAndroidKey)) {

    let androidDownloadStream = s3.getObject({
      Bucket: modelsBucket,
      Key: multiarchiveAndroidKey
    })
    .createReadStream();

    let parseAndroid = new tar.Parse({ filter: modelNameFileFilter })
    .on('entry', function(entry) {
      packAndroid.add(entry);
    });

    await pipeline(androidDownloadStream, parseAndroid);
  }

  // Add new model to archives
  let parseNewModel = new tar.Parse({
    // Skip hidden macOS files which may cause problems during renaming
    filter: (path) => !path.startsWith('.')
  })
  .on('entry', function(entry) {
    // Rename model entries to prevent name conflicts
    // File names should be equal the model name + extension
    const extension = entry.path.split('.').pop();
    entry.header.path = modelName + '.' + extension;
    entry.path = entry.header.path;

    // Entries will be filtered during packing
    packiOS.add(entry);
    packAndroid.add(entry);
  })
  .on('finish', function() {
    packiOS.end();
    packAndroid.end();
  });

  let newModelStream = s3.getObject({
    Bucket: s3Record.bucket.name,
    Key: inputKey,
  })
  .createReadStream();

  await pipeline(newModelStream, parseNewModel);

  // Uploading the result
  let uploadTasks = [];
  uploadTasks.push({
    stream: packiOS,
    key: multiarchiveiOSKey,
    copyKey: historyKeys['mlmodel']
  });
  uploadTasks.push({
    stream: packAndroid,
    key: multiarchiveAndroidKey,
    copyKey: historyKeys['xgb']
  });

  let writePromises = [];
  for (const task of uploadTasks) {
    /* There is a bug with upload() and putObject() so we can't
     use tar Pack stream directly with them, but we can pipe it
     to an intermediate PassThrough stream. */
    let passthroughStream = new stream.PassThrough();

    const writeParams = {
      Body: passthroughStream,
      Bucket: modelsBucket,
      Key: task.key
    };
    const copyParams = {
      Bucket: modelsBucket,
      CopySource: modelsBucket + '/' + task.key,
      Key: task.copyKey
    };
    let writePromise = s3.upload(writeParams).promise()
    .then(
      function(data) {
        return s3.copyObject(copyParams).promise();
      }
    );
    writePromises.push(writePromise);

    let readStream = task.stream;
    readStream.pipe(passthroughStream);
  }

  return Promise.all(writePromises);
}

async function isFieExists(bucket, key) {
  try {
    let data = await s3.headObject({
      Bucket: bucket,
      Key: key
    })
    .promise();

    return data != null;

  } catch(e) {
    if (e.code == 'NotFound' && e.statusCode == 404) {
      return false
    } else {
      throw e;
    }
  }
}

/*
 Make model keys by adding timestamped filenames to the path.
 Output - JSON:
 {
   mlmodel: improve-mlmodel-year-month-day-hour-minute-second-uuid.tar.gz,
   xgb: improve-xgb-year-month-day-hour-minute-second-uuid.tar.gz
 }
 */
function makeTimestampedKeys(path) {
  const now = new Date();
  let dateStr = `${now.getUTCFullYear()}-${now.getUTCMonth()}-${now.getUTCDate()}-`;
  dateStr += `${now.getUTCHours()}-${now.getUTCMinutes()}-${now.getUTCSeconds()}`;

  const uuidStr = uuidv4();

  if (path.substr(-1) != '/') {
    path += '/';
  }

  output = {};
  const types = ['mlmodel', 'xgb'];
  for (const type of types) {
    let name = 'improve-' + type + '-' + dateStr + '-' + uuidStr + '.tar.gz';
    output[type] = path + type + '/' + name;
  }
  return output;
}
