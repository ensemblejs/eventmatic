'use strict';

var AWS = require('aws-sdk');
var s3 = new AWS.S3();

var params = {
  Bucket: 'ensemblejs-events',
  Prefix: 'in/event-capture'
};

s3.listObjects(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});