'use strict';
var AWS = require('aws-sdk');
var async = require('async');

AWS.config.update({ region: "us-east-2" });

var s3 = new AWS.S3();
var dynamo = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();

var _tableName = "jkwIoTSample";

function QuerySample(batchId, runTime) {
    try {
        var filterExpersion = "BatchID = :batchId and BatchRunDate = :runTime";
        var runTimeDate = new Date(runTime);
        var runDate = runTimeDate.getDate();
        var start = runDate - 1,
            end = runDate + 1;

        var dynamoRequest = {
            TableName: _tableName,
            ProjectionExpression: "BatchID, BatchRunTime, RBC, RBC_average, RBC_sd",
            FilterExpression: filterExpersion,
            ExpressionAttributeValues: {
                ":batchId": batchId,
                ":runTime" : runTime
            }
        };

        var results = [];
        docClient.scan(dynamoRequest, function (err, data) {
                debugger;
                if (err) {
                    console.error("error: " + err);
                    return null;
                } else {
                    console.log("response length: " + data.Items.length);
                    var max = (data.Items.length < 30) ? data.Items.length : 30;
                    var accumulator = 0;
                    for (var counter = 0; counter < max; counter++) {
                        results.push(data.Items[counter]);
                        accumulator += data.Items[counter].RBC;
                    }

                    //results has the data - calculate the mean and standard deviation
                    var mean = accumulator / max;
                    var variance = [];
                    var varianceTotal = 0;

                    results.forEach(function(item) {
                        var v = item.RBC - mean;
                        var vs = v * v;
                        variance.push(vs);
                        varianceTotal += vs;
                    });

                    var standardDeviation = varianceTotal / (max - 1);
                    var result = {
                        "average": mean,
                        "standard_deviation": standardDeviation,
                    }
                    return result;
                }
            }
        );
    } catch (err) {
        console.error("error: " + err);
    }
}

QuerySample("22570345", "2017/9/26");