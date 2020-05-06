'use strict';

const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const zlib = require('zlib')
const es = require('event-stream')

const me = module.exports

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

        results.push(mapFunction(record))
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

module.exports.compressAndWriteBuffers = (s3Key, buffers) => {
  console.log(`writing ${buffers.length} records to ${s3Key}`)
  
  let params = {
    Body: zlib.gzipSync(Buffer.concat(buffers)),
    Bucket: process.env.RECORDS_BUCKET,
    Key: s3Key
  }

  return s3.putObject(params).promise()
}

module.exports.deleteAllKeys = (s3Keys) => {
  const promises = []
  for (const s3Key of s3Keys) {
    promises.push(module.exports.deleteKey(s3Key))
  }
  return Promise.all(promises)
}

module.exports.deleteKey = (s3Key) => {
  console.log(`deleting ${s3Key}`)
  let params = {
    Bucket: process.env.RECORDS_BUCKET,
    Key: s3Key
  }
  return s3.deleteObject(params).promise()
}

module.exports.listAllPrefixes = (params, depth=1) => {
  return me.listAllSubPrefixes(params).then(subDirectories => {
    if (depth <= 1) {
      return subDirectories.map(subDirectory => params.Prefix + subDirectory + params.Delimiter) 
    } else {
      // it is important to clone the params with Object.create() since the same params object is used multiple times in parallel
      return Promise.all(subDirectories.map(subDirectory => me.listAllPrefixes(Object.assign(Object.create(params), {Prefix: params.Prefix + subDirectory + params.Delimiter}), depth-1))).then(all => all.flat())
    }
  })
}

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
module.exports.listAllSubPrefixes = (params, out = []) => new Promise((resolve, reject) => {
  console.log(`listing subdirectories for ${params.Prefix}`)
  s3.listObjectsV2(params).promise()
    .then(({CommonPrefixes, IsTruncated, NextContinuationToken}) => {
      out.push(...CommonPrefixes.map(o => o.Prefix.split('/').slice(-2)[0])); // split and grab the second to last item from the Prefix
      !IsTruncated ? resolve(out) : resolve(me.listAllSubPrefixes(Object.assign(Object.create(params), {ContinuationToken: NextContinuationToken}), out)); // Object.create() to clone incase used in parallel
    })
    .catch(reject);
});

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
module.exports.listAllKeys = (params, out = []) => new Promise((resolve, reject) => {
  console.log(`listing all keys for ${params.Prefix}`)
  s3.listObjectsV2(params).promise()
    .then(({Contents, IsTruncated, NextContinuationToken}) => {
      out.push(...Contents.map(o => o.Key));
      !IsTruncated ? resolve(out) : resolve(me.listAllKeys(Object.assign(Object.create(params), {ContinuationToken: NextContinuationToken}), out)); // Object.create() to clone incase used in parallel
    })
    .catch(reject);
});

// modified from https://stackoverflow.com/questions/42394429/aws-sdk-s3-best-way-to-list-all-keys-with-listobjectsv2
module.exports.listAllKeysMetadata = (params, out = []) => new Promise((resolve, reject) => {
  console.log(`listing metadata for all keys for ${params.Prefix}`)
  s3.listObjectsV2(params).promise()
    .then(({Contents, IsTruncated, NextContinuationToken}) => {
      out.push(...Contents);
      !IsTruncated ? resolve(out) : resolve(me.listAllKeysMetadata(Object.assign(Object.create(params), {ContinuationToken: NextContinuationToken}), out)); // Object.create() to clone incase used in parallel
    })
    .catch(reject);
});
