'use strict';
var AWS = require('aws-sdk');
var async = require('async');

AWS.config.update({ region: "us-east-2" });

var s3 = new AWS.S3();
var dynamo = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();

var _tableName = "jkwIoTSample2";
var _sourceBucket = "jkw-test-bucket";
var _destinationBucket = "hcciotpoc-jkw";

var samples = [];

function s3ListObjects(cb) {
    var params = {
        Bucket: _sourceBucket
    };

    s3.listObjects(params, function(err, data) {
        var sampleKeys = [];
        if (err) {
            console.error("error: " + err);
        } else {
            var items = data.Contents;
            items.forEach(function(item) {
                sampleKeys.push(item);
            });
            if (true == data.IsTruncated) {
                //s3ListObjects(cb);
            }
            cb(sampleKeys);

        }
    });
}

//debugger;

s3ListObjects(function(sampleKeys) {
    console.log("moving " + sampleKeys.length + " samples to test bucket");
    sampleKeys.forEach(function (sample) {
        var key = sample.Key;
        var getParam = {
            Bucket: _sourceBucket,
            Key: key
        };
        console.log("get: " + key);
        s3.getObject(getParam, function (err, s3Object) {

            var putParam = {
                Bucket: _destinationBucket,
                Key: key,
                Body: s3Object.Body
            };
            s3.putObject(putParam, function(err, response) {
                console.log("uploaded: " + key);
            });
        });
    });
    /*
    samples.forEach(function (sample) {
        var uploadParam = {
            Bucket: 'hcciotpoc',
            Key: fileName,
            Body: contents
        }
    });
    */
});