const pulumi = require("@pulumi/pulumi");
const aws = require("@pulumi/aws");
const awsx = require("@pulumi/awsx");
const turf = require("@turf/helpers");
const elevation = require("terrain-rgb-query");
const fs = require("fs");
const axios = require("axios");

const config = new pulumi.Config();

const tags = {
  Owner: "ADD_OWNER_TAG",
  Customer: "Public",
  UseCase: "ADD_DESCRIPTIVE_TAG",
};

//* Set your AWS IoT Endpoint - this is used to generate the IoTHarness
const IoTEndpoint = aws.iot.getEndpoint({ endpointType: "iot:Data-ATS" })
  .endpointAddress;

//* Set your Mapbox token for use in the elevation query
//* This token can be used to call any Mapbox API
const mapboxToken = config.require("token");

//* Create S3 Bucket
//* This is where the all the ingested data ends up.
const publicBucket = new aws.s3.Bucket("publicBucket", { tags: tags });

//* Create Dynamo Table
//* This manages the state and location of all assets
const assetTable = new aws.dynamodb.Table("publicTable", {
  attributes: [
    {
      name: "id",
      type: "N",
    },
  ],
  hashKey: "id",
  billingMode: "PAY_PER_REQUEST",
  tags: tags,
});

//* Create API to read Dynamo
//* This will scan the table and convert it to geojson for consumption via GL-JS
const endpoint = new awsx.apigateway.API("publicQuery", {
  routes: [
    {
      path: "/",
      method: "GET",
      eventHandler: (request, ctx, cb) => {
        const AWS = require("aws-sdk");
        const ddb = new AWS.DynamoDB.DocumentClient({
          apiVersion: "2012-10-08",
        });
        const tableName = assetTable.name.value;
        const params = {
          TableName: tableName,
        };
        ddb.scan(params, (err, data) => {
          if (data.Items.length > 0) {
            //All data processing occurs here
            const features = data.Items.map((item) => {
              let featureParams = {};
              let itemKeys = Object.keys(item);
              let staticParams = ["latitude", "longitude"];
              const buildKeys = itemKeys.filter(
                (key) => !staticParams.includes(key)
              );
              buildKeys.forEach((key) => {
                featureParams[key] = item[key];
              });
              const point = turf.point(
                [item.longitude, item.latitude],
                featureParams
              );
              return point;
            });
            const featureCollection = turf.featureCollection(features);
            //Data is transmitted to the client here
            cb(undefined, {
              statusCode: 200,
              body: Buffer.from(
                JSON.stringify(featureCollection),
                "utf8"
              ).toString("base64"),
              isBase64Encoded: true,
              headers: {
                "content-type": "application/json",
                "Access-Control-Allow-Origin": "*",
              },
            });
          } else {
            const message = { message: "No assets are currently available." };
            cb(undefined, {
              statusCode: 200,
              body: Buffer.from(JSON.stringify(message), "utf8").toString(
                "base64"
              ),
              isBase64Encoded: true,
              headers: {
                "content-type": "application/json",
                "Access-Control-Allow-Origin": "*",
              },
            });
          }
        });
      },
    },
  ],
  stageName: "dev",
  stageArgs: {
    tags: tags,
  },
});

//* Create Kinesis stream for ingestion
//* All data from IoT Core is forwarded to this stream
const ingestStream = new aws.kinesis.Stream("publicStream", {
  //To increase scale, increase your shard count
  shardCount: 1,
  retentionPeriod: 72,
  tags: tags,
});

//* Create IoT Rule to push into Kinesis stream
//* This forwards all data on the specific channel into Kinesis
const iotRole = new aws.iam.Role("publicIotRole", {
  assumeRolePolicy: JSON.stringify({
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Principal: {
          Service: "iot.amazonaws.com",
        },
        Action: "sts:AssumeRole",
      },
    ],
  }),
  tags: tags,
});

const iotRolePolicy = new aws.iam.RolePolicy("publicIotRolePolicy", {
  policy: pulumi.interpolate`{
    "Version": "2012-10-17",
    "Statement": [
      {
          "Effect": "Allow",
          "Action": [
              "kinesis:*"
          ],
          "Resource": "${ingestStream.arn}"
      }
    ]
  }`,
  role: iotRole.id,
});

const iotRule = new aws.iot.TopicRule("publicIotAssetIngest", {
  name: "publicAssetIngest",
  description: "Pass from IoT Core to Asset Tracking",
  enabled: true,
  kinesis: {
    partitionKey: "id",
    roleArn: iotRole.arn,
    streamName: ingestStream.name,
  },
  //Select only the public items from the ingestion stream
  //* Update these if you want more data to come through on the public side
  sql: "SELECT id,coordinates,timestamp FROM 'assetingest'",
  sqlVersion: "2015-10-08",
});

//* Create Firehose and associated IAM roles to accept your archived data
//* Firehose will collect and deposit enriched data into S3
const firehoseIAMRole = new aws.iam.Role("publicFirehoseRole", {
  assumeRolePolicy: JSON.stringify({
    Version: "2012-10-17",
    Statement: [
      {
        Action: "sts:AssumeRole",
        Principal: {
          Service: "firehose.amazonaws.com",
        },
        Effect: "Allow",
        Sid: "",
      },
    ],
  }),
  tags: tags,
});

const firehoseIAMRolePolicy = new aws.iam.RolePolicy(
  "publicFirehoseRolePolicy",
  {
    role: firehoseIAMRole.id,
    policy: publicBucket.arn.apply((arn) => {
      const policy = JSON.stringify({
        Version: "2012-10-17",
        Statement: [
          {
            Sid: "",
            Effect: "Allow",
            Action: [
              "s3:AbortMultipartUpload",
              "s3:GetBucketLocation",
              "s3:GetObject",
              "s3:ListBucket",
              "s3:ListBucketMultipartUploads",
              "s3:PutObject",
            ],
            Resource: [arn, `${arn}/*`],
          },
          {
            Sid: "",
            Effect: "Allow",
            Action: [
              "kinesis:DescribeStream",
              "kinesis:GetShardIterator",
              "kinesis:GetRecords",
            ],
            Resource: "*",
          },
        ],
      });
      return policy;
    }),
  }
);

const ingestFirehose = new aws.kinesis.FirehoseDeliveryStream(
  "publicFirehose",
  {
    destination: "extended_s3",
    extendedS3Configuration: {
      bucketArn: publicBucket.arn,
      bufferInterval: 600,
      //This defines the size of output written to S3. Larger = bigger payload.
      bufferSize: 25,
      compressionFormat: "GZIP",
      roleArn: firehoseIAMRole.arn,
    },
    tags: tags,
  }
);

//* Create Lambda to process data in Kinesis
//* This will push to Dynamo, Firehose, and IoT for realtime front-end updates
const kinesisLambdaRole = new aws.iam.Role("publicKinesisLambdaRole", {
  assumeRolePolicy: JSON.stringify({
    Version: "2012-10-17",
    Statement: [
      {
        Action: "sts:AssumeRole",
        Principal: {
          Service: "lambda.amazonaws.com",
        },
        Effect: "Allow",
        Sid: "",
      },
    ],
  }),
  tags: tags,
});

const kinesisLambdaRolePolicy = new aws.iam.RolePolicy(
  "publicKinesisLambdaRolePolicy",
  {
    role: kinesisLambdaRole.id,
    policy: pulumi
      .all([ingestStream.arn, assetTable.arn, ingestFirehose.arn])
      .apply(([ingestStream, assetTable, ingestFirehose]) => {
        const lambdaPolicy = JSON.stringify({
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
              ],
              Resource: "*",
            },
            {
              Effect: "Allow",
              Action: "kinesis:*",
              Resource: ingestStream,
            },
            {
              Effect: "Allow",
              Action: "dynamodb:*",
              Resource: assetTable,
            },
            {
              Effect: "Allow",
              Action: "firehose:*",
              Resource: ingestFirehose,
            },
          ],
        });
        return lambdaPolicy;
      }),
  }
);

const kinesisLambda = new aws.lambda.CallbackFunction("publicStreamProcessor", {
  role: kinesisLambdaRole,
  runtime: "nodejs12.x",
  timeout: 30,
  callback: async (event, context, callback) => {
    const AWS = require("aws-sdk");
    const ddb = new AWS.DynamoDB.DocumentClient({
      apiVersion: "2012-10-08",
    });
    const fh = new AWS.Firehose();
    const template = `https://api.mapbox.com/v4/mapbox.terrain-rgb/{z}/{x}/{y}.pngraw?pluginName=ATSolution&access_token=${mapboxToken}`;
    const elevationQuery = new elevation.TerrainRGBquery(template);
    for (const [i, record] of event.Records.entries()) {
      //All data processing occurs here
      //This loop applies to each record ingested by Lambda from Kinesis
      console.log("Reading Data");
      const item = record.kinesis.data;
      const b = new Buffer.from(item, "base64").toString("utf-8");
      const parsedData = JSON.parse(b);
      const parsedKeys = Object.keys(parsedData);

      console.log("Parsing Data");
      const coordinates = parsedKeys.includes("coordinates")
        ? parsedData.coordinates
        : null;
      let params = { TableName: assetTable.id.get(), Item: {} };
      let staticParams = ["timestamp", "coordinates", "id"];

      console.log("Building Params");
      const buildKeys = parsedKeys.filter((key) => !staticParams.includes(key));
      params.Item.ts = parsedData.timestamp;
      if (coordinates) {
        params.Item.elevation = await elevationQuery.queryElevation(
          coordinates
        );
      }
      params.Item.longitude = coordinates !== null ? coordinates[0] : null;
      params.Item.latitude = coordinates !== null ? coordinates[1] : null;
      params.Item.id = parsedData.id;
      buildKeys.forEach((key) => {
        params.Item[key] = parsedData[key];
      });

      console.log("Writing to Dynamo");
      const dynamoStatus = await ddb.put(params).promise();
      console.log(dynamoStatus);

      console.log("Writing to Firehose");
      const fhStatus = await fh
        .putRecord({
          DeliveryStreamName: ingestFirehose.name.get(),
          Record: { Data: new Buffer.from(JSON.stringify(params.Item)) },
        })
        .promise();
      console.log(fhStatus);
    }
  },
  tags: tags,
});

const kinesisLambdaEventMapping = new aws.lambda.EventSourceMapping(
  "publicMap2Kinesis",
  {
    //This defines how many records to pick up per Lambda invocation.
    //You will need to align this against ingestion volume and processing time.
    batchSize: 25,
    enabled: true,
    eventSourceArn: ingestStream.arn,
    functionName: kinesisLambda.name,
    startingPosition: "LATEST",
  }
);

//* This section writes all the necessary files for testing and validation
const harnessCert = new aws.iot.Certificate("harnessCert", {
  active: true,
});

const harnessPolicy = new aws.iot.Policy("harnessPolicy", {
  policy: JSON.stringify({
    Version: "2012-10-17",
    Statement: [
      {
        Action: ["iot:*"],
        Effect: "Allow",
        Resource: "*",
      },
    ],
  }),
});

const harnessAttachment = new aws.iot.PolicyAttachment("harnessAttachment", {
  policy: harnessPolicy.name,
  target: harnessCert.arn,
});

harnessCert.privateKey.apply((key) => {
  fs.writeFileSync("../IoTHarness/harness-private.pem.key", key);
});

harnessCert.certificatePem.apply((cert) => {
  fs.writeFileSync("../IoTHarness/harness-certificate.pem.crt", cert);
});

const harnessThing = new aws.iot.Thing("harness", {});

const harnessCertAttach = new aws.iot.ThingPrincipalAttachment(
  "harnessThingAttach",
  {
    principal: harnessCert.arn,
    thing: harnessThing.name,
  }
);

axios
  .get("https://www.amazontrust.com/repository/AmazonRootCA1.pem")
  .then((response) => {
    fs.writeFileSync("../IoTHarness/AmazonRootCA1.pem", response.data);
  });

const harness = fs.readFileSync("../IoTHarness/init.js").toString();
const newHarness = harness.replace("$$INSERT$$", IoTEndpoint);
fs.writeFileSync("../IoTHarness/index.js", newHarness);

endpoint.url.apply((endpoint) => {
  const oldHTML = fs.readFileSync("../frontEnd/init.html").toString();
  const newHTML = oldHTML.replace("$$$PULUMIURL$$$", endpoint);
  const newHTMLToken = newHTML.replace("$$MAPBOX$$", mapboxToken);
  fs.writeFileSync("../frontEnd/index.html", newHTMLToken);
});

//* This exports the URL that queries Dynamo
exports.url = endpoint.url;
