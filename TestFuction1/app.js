
var AWS = require("aws-sdk");
var async = require("async");

console.log('Loading');

var s3 = new AWS.S3();
var dynamo = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();

var _tableName = "jkwIoTSample";


exports.TestFuncthandler = function (event, context) {
    console.log("jkw test function ");

    if (event != null) {
        var srcBucket = event.Records[0].s3.bucket.name;
        console.log("bucket name: " + srcBucket);

        var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
        console.log("key: " + srcKey);

        var longKey = srcBucket + "/" + srcKey;
        var newSample = null;
        var batchDate = "";
        var key = "";

        async.waterfall([
            function download(next) {
                console.log("Get the new sample from the bucket");
                var request = {
                    Bucket: srcBucket,
                    Key: srcKey
                };

                s3.getObject(request,next);
            },
            function transform(response, next) {
                console.log("transform sample to dynamoDB schema");

                var rawData = response.Body.toString('utf-8');
                newSample = JSON.parse(rawData);
                
                next(null, newSample);
            },
            function getSamples(data, next) {
                var filterExpersion = "BatchID = :batchId and BatchRunDate = :runTime";
                var batchRunTime = new Date(data.BatchRunTime);

                batchDate = batchRunTime.getFullYear() + "/" + (1 + batchRunTime.getMonth()) + "/" + batchRunTime.getDate();

                var dynamoRequest = {
                    TableName: _tableName,
                    ProjectionExpression: "BatchID, BatchRunDate, RBC, RBC_average, RBC_StandardDeviation",
                    FilterExpression: filterExpersion,
                    ExpressionAttributeValues: {
                        ":batchId": data.BatchID,
                        ":runTime": batchDate
                    }
                };

                docClient.scan(dynamoRequest, next);
            },
            function calculateValuesAndStore(data, next) {
                console.log("response length: " + data.Items.length);
                var max = (data.Items.length < 30) ? data.Items.length : 30;
                var accumulator = 0;
                var results = [];
                for (var counter = 0; counter < max; counter++) {
                    results.push(data.Items[counter]);
                    accumulator += data.Items[counter].RBC;
                }

                //results has the data - calculate the mean and standard deviation

                debugger;

                var mean = 0;
                var variance = [];
                var varianceTotal = 0;
                var standardDeviation = 0;
                
                if (1 > max) {
                    mean = 0;
                    standardDeviation = 0;
                } else {
                    results.forEach(function (item) {
                        var v = item.RBC - mean;
                        var vs = v * v;
                        variance.push(vs);
                        varianceTotal += vs;
                    });
                    standardDeviation = varianceTotal / (max - 1);
                }

                var key = newSample.AnalyzerModel + "-" + newSample.AnalyzerSerial + "-" + newSample.BatchID + "-" + newSample.BatchRunTime;
                var dynamoRequest = {
                    TableName: _tableName,
                    Item: {
                        "RawSource": { "S": key.toString() },
                        "Source": { "S": longKey },
                        "RawDataID": { "S": newSample.RawDataID.toString() },
                        "BatchID": { "S": newSample.BatchID.toString() },
                        "OrderBy": { "N": newSample.OrderBy.toString() },
                        "SampleDate": { "S": newSample.SampleDate.toString() },
                        "BatchRunDate": { "S": batchDate },
                        "BatchRunTime": { "S": newSample.BatchRunTime.toString() },
                        "SiteID": { "N": newSample.SiteID.toString() },
                        "AnalyzerID": { "N": newSample.AnalyzerID.toString() },
                        "AnalyzerModel": { "S": newSample.AnalyzerModel.toString() },
                        "AnalyzerSerial": { "S": newSample.AnalyzerSerial.toString() },
                        "ControlID": { "N": newSample.ControlID.toString() },
                        "MaterialType": { "S": newSample.MaterialType.toString() },
                        "LotNumber": { "N": newSample.LotNumber.toString() },
                        "LevelCode": { "N": newSample.LevelCode.toString() },
                        "ModeCode": { "S": newSample.ModeCode.toString() },
                        "WBC": { "N": newSample.WBC.toString() },
                        "RBC": { "N": newSample.RBC.toString() },
                        "RBC_average": { "N": mean.toString() },
                        "RBC_StandardDeviation": { "N": standardDeviation.toString() },
                        "HGB": { "N": newSample.HGB.toString() },
                        "HCT": { "N": newSample.HCT.toString() },
                        "MCV": { "N": newSample.MCV.toString() },
                        "MCH": { "N": newSample.MCH.toString() },
                        "MCHC": { "N": newSample.MCHC.toString() },
                        "PLT": { "N": newSample.PLT.toString() },
                        "RDWSD": { "N": newSample.RDWSD.toString() }
                    }
                };

                dynamo.putItem(dynamoRequest, next);
            }
        ],
            function (err, result) {
                if (null !== err) {
                    console.error('unable to process sample ' + srcKey + " error: " + err);
                } else {
                    console.log("results: " + result);
                }
                
            });
    }
    else {
        console.log('No event object');
    }
};

