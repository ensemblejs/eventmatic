'use strict';

var express = require('express');
var bodyParser = require('body-parser');
var map = require('lodash').map;
var uniq = require('lodash').uniq;
var pluck = require('lodash').pluck;
var first = require('lodash').first;
var select = require('lodash').select;
var sortBy = require('lodash').sortBy;
var AWS = require('aws-sdk-promise');
var Bluebird = require('bluebird');
var cors = require('cors');
var s3 = new AWS.S3();

if (!process.env.AWS_PROFILE) {
  console.error('AWS_PROFILE not set.');
  process.exit();
}

var corsOptions = {
  origin: '*',
  methods: ['POST'],
  allowedHeaders: ['content-type'],
  credentials: true,
  preflightContinue: true
};

var app = express();
app.use(bodyParser.json({limit: '5mb'}));
app.use(bodyParser.urlencoded({ extended: true , limit: '5mb'}));

var port = process.env.PORT || 8080;

function stripPrefix (params) {
  return function (keys) {
    return map(keys, key => key.replace(params.Prefix, ''));
  };
}

function getMatchingRecords (params) {
  return s3.listObjects(params).promise()
    .then(req => req.data.Contents)
    .then(fileList => pluck(fileList, 'Key'))
    .then(fileList => map(fileList, file => {
      return {Bucket: 'ensemblejs-events', Key: file};
    }))
    .then(requestParams => map(requestParams, param => {
      return s3.getObject(param).promise().then(function (req) {
        return JSON.parse(req.data.Body);
      });
    }))
    .then(Bluebird.all);
}

function findLatest (params) {
  return s3.listObjects(params).promise()
    .then(req => req.data.Contents)
    .then(fileList => pluck(fileList, 'Key'))
    .then(stripPrefix(params))
    .then(fileList => map(fileList, file => file.split('/')[1]))
    .then(uniq)
    .then(records => records.reverse())
    .then(first);
}

function getDataFromS3 (req, res) {
  var appId = req.params.appId;
  var eventName = req.params.eventName;
  var timeFilter = req.params.timeFilter;
  var params;

  function applyFilters (records) {
    return select(records, req.query);
  }

  if (timeFilter !== 'latest') {
    params = {
      Bucket: 'ensemblejs-events',
      Prefix: ['in', appId, eventName, timeFilter].join('/')
    };

    return getMatchingRecords(params)
      .then(applyFilters)
      .then(data => res.json(data))
      .catch(err => console.error(err));
  } else {
    params = {
      Bucket: 'ensemblejs-events',
      Prefix: ['in', appId, eventName].join('/')
    };

    return findLatest(params)
      .then(timestamp => {
        return {
          Bucket: 'ensemblejs-events',
          Prefix: ['in', appId, eventName, timestamp].join('/')
        };
      })
      .then(getMatchingRecords)
      .then(applyFilters)
      .then(data => res.json(data))
      .catch(err => console.error(err));
  }
}

app.options('*', cors(corsOptions));
app.get('/:appId/:eventName/:timeFilter', cors(corsOptions), getDataFromS3);

app.listen(port, function () {
  var versionInfo = require('../package.json');
  console.log('%s@%s listening on %s', versionInfo.name, versionInfo.version, port);
});
