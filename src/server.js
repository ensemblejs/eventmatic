'use strict';

var Bluebird = require('bluebird');
var AWS = require('aws-sdk-promise');
var s3 = new AWS.S3();
var map = require('lodash').map;
var sortBy = require('lodash').sortBy;
var pluck = require('lodash').pluck;
var uuid = require('node-uuid');

var params = {
  Bucket: 'ensemblejs-events',
  Prefix: 'in/event-capture'
};


function withList (req) {
  var dataPoints = [];

  var promises = map(req.data.Contents, function (s3Item) {
    var objParam = {Bucket: 'ensemblejs-events', Key: s3Item.Key};
    return s3.getObject(objParam).promise().then(function (req) {
      var payload = JSON.parse(req.data.Body);

      return {
        duration: payload.duration,
        timestamp: payload.timestamp,
        count: 1
      };
    });
  });

  return Bluebird.all(promises)
}

function sortByTimestamp (dataPoints) {
  return sortBy(dataPoints, 'timestamp');
}

function prepareForUpload (dataPoints) {
  return {
    average: pluck(dataPoints, 'duration'),
    interval: pluck(dataPoints, 'timestamp'),
    count: pluck(dataPoints, 'count'),
  };
}

function uploadToS3 (content) {
  var opts = {
    Bucket: 'ensemblejs-events',
    Key: 'agg/event-save-duration.json',
    Body: JSON.stringify(content)
  };

  s3.upload(opts).send(function (err, req) {
    if (err) {
      console.error(err);
      return;
    }

    console.log(req);
  });
}

s3.listObjects(params).promise()
  .then(withList)
  .then(sortByTimestamp)
  .then(prepareForUpload)
  .then(uploadToS3);