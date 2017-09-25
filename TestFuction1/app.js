
var AWS = require("aws-sdk");
var async = require("async");

console.log('Loading');

var s3 = new AWS.S3();
var dynamo = new AWS.DynamoDB();

var tableName = "jkwIoTSample";


exports.TestFuncthandler = function (event, context) {
    debugger;
    console.log("jkw test function ");

    if (event != null) {

        //TODO: validate that the event object is setup properly

        var srcBucket = event.Records[0].s3.bucket.name;
        console.log("bucket name: " + srcBucket);

        var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
        console.log("key: " + srcKey);

        var longKey = srcBucket + "/" + srcKey;

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
                var data = JSON.parse(rawData);
                
                next(null, data);
            },
            function store(data, next) {
                if (null == data) {
                    next("no sample data provided");
                }
                console.log('store data in dynamoDB');

                var key = longKey + "/" + data.RawDataID;

                var dynamoRequest = {
                    TableName: tableName,
                    Item: {
                        "RawSource": { "S": longKey + "/" + data.RawDataID },
                        "Source": { "S": longKey },
                        "RawDataID": { "S": data.RawDataID.toString() },
                        "BatchID": { "S": data.BatchID.toString() },
                        "OrderBy": { "N": data.OrderBy.toString() },
                        "SampleDate": { "S": data.SampleDate.toString() },
                        "BatchRunTime": { "S": data.BatchRunTime.toString() },
                        "SiteID": { "N": data.SiteID.toString() },
                        "AnalyzerID": { "N": data.AnalyzerID.toString() },
                        "AnalyzerModel": { "S": data.AnalyzerModel.toString() },
                        "AnalyzerSerial": { "S": data.AnalyzerSerial.toString() },
                        "ControlID": { "N": data.ControlID.toString() },
                        "MaterialType": { "S": data.MaterialType.toString() },
                        "LotNumber": { "N": data.LotNumber.toString() },
                        "LevelCode": { "N": data.LevelCode.toString() },
                        "ModeCode": { "S": data.ModeCode.toString() },
                        "WBC": { "N": data.WBC.toString() },
                        "RBC": { "N": data.RBC.toString() },
                        "HGB": { "N": data.HGB.toString() },
                        "HCT": { "N": data.HCT.toString() },
                        "MCV": { "N": data.MCV.toString() },
                        "MCH": { "N": data.MCH.toString() },
                        "MCHC": { "N": data.MCHC.toString() },
                        "PLT": { "N": data.PLT.toString() },
                        "RDWSD": { "N": data.RDWSD.toString() }                    }
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

