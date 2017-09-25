'use strict';

var AWS = require('aws-sdk');
var async = require('async');
var fs = require('fs');

var s3 = new AWS.S3();

function ReadFile(filename) {
    var fileContents = fs.readFileSync(filename, 'utf8');

    return fileContents;
}

var fileName = '';
if (2 >= process.argv.length) {
    console.log('Useage: TestFunctDriver file');
    //process.exit(-1);
    fileName = 'JKW-TEST-3.txt'; // VS isnt letting me pass command line parameters so we're faking it.
} else {
    fileName = process.argv[2];
}

console.log("file: " + fileName);
var contents = ReadFile(fileName);

var s3Parameters = {
    Bucket: 'hcciotpoc',
    Key: fileName,
    Body: contents
};

s3.putObject(s3Parameters,
    function(err, response) {
        if (err) {
            console.error("error: " + err);
            process.exit(-1);
        }

        console.log("file uploaded");
    });
