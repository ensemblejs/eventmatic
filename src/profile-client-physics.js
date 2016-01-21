'use strict';

//jshint node: true

var Bluebird = require('bluebird');
var AWS = require('aws-sdk-promise');
var s3 = new AWS.S3();
var map = require('lodash').map;
var sortBy = require('lodash').sortBy;
var pluck = require('lodash').pluck;
var select = require('lodash').select;
var each = require('lodash').each;
var bucket = 'ensemblejs-events';
var appId = 'distributedlife+pong';
var params = {
  Bucket: bucket,
  Prefix: 'in/' + appId
};

if (!process.env.AWS_PROFILE) {
  console.error('AWS_PROFILE not set.');
  process.exit();
}


function getData (req) {
  var promises = map(req.data.Contents, function (s3Item) {
    var objParam = {Bucket: bucket, Key: s3Item.Key};
    return s3.getObject(objParam).promise().then(function (req) {
      return JSON.parse(req.data.Body);
    });
  });

  return Bluebird.all(promises);
}

function uploadToS3 (records) {
  var opts = {
    Bucket: bucket,
    Key: 'agg/' + appId + '-profile-client-physics.json',
    Body: JSON.stringify(records)
  };

  s3.upload(opts).send(function (err, req) {
    if (err) {
      console.error(err);
      return;
    }

    console.log(req);
  });

  return records;
}

function startsWith (records, key) {
  return select(records, record => {
    return record.name.startsWith(key);
  });
}

function print (records) {
  console.log(records);
}

var key = 'ensemblejs.BeforePhysicsFrame.ProcessPendingInput.noInput';
var value = '95th';

s3.listObjects(params).promise()
  .then(getData)
  .then(records => startsWith(records, key))
  .then(records => sortBy(records, 'created'))
  .then(records => map(records, record => {
    return {
      'name': record.name.replace(key, ''),
      'value': record[value]
    };
  }))
  .then(records => {
    var byName = {};
    each(pluck(records, 'name'), name => byName[name] = []);

    each(records, record => {
      byName[record.name].push(record.value);
    });

    return byName;
  })
  .then(uploadToS3)
  .catch(err => console.error(err));