const pulumi = require("@pulumi/pulumi");
const aws = require("@pulumi/aws");
const awsx = require("@pulumi/awsx");
const random = require("@pulumi/random");
const turf = require("@turf/helpers");
const elevation = require("terrain-rgb-query");
const fs = require("fs");
const axios = require("axios");
const yaml = require("js-yaml");

const config = new pulumi.Config();

//* Set your AWS IoT Endpoint - this is used to generate the IoTHarness
const IoTEndpoint = aws.iot.getEndpoint({ endpointType: "iot:Data-ATS" })
  .endpointAddress;

//* Set your Mapbox token for use in the elevation query
//* This token can be used to call any Mapbox API
const mapboxToken = config.require("token");
//* Set the private identifier and customer name, to filter the data appropriately
const identifier = config.require("identifier");
const customer = config.require("customer");
//* Set the Rockset token to create the collection and write into it
const rocksetToken = config.requireSecret("rockset");

//* Check if the collection exists. If not, create it.
const rocksetTest = async (token) => {
  const collectionName = `${customer}Rebelle`;
  const apiserver = "https://api.rs2.usw2.rockset.com";
  const rockset = require("rockset").default(token, apiserver);
  const listCollections = await rockset.collections.listCollections("commons");
  const collectionsTest = listCollections.data.filter(
    (item) => item.name === collectionName
  );
  if (collectionsTest.length != 1) {
    console.log("Time to create a collection");
    const mappings = [
      {
        name: "date_to_event_time",
        input_fields: [
          {
            field_name: "input_ts",
            if_missing: "SKIP",
            is_drop: true,
            param: "input_timestamp",
          },
        ],
        output_field: {
          field_name: "_event_time",
          on_error: "FAIL",
          value: {
            sql: "CAST(:input_timestamp as timestamp)",
          },
        },
      },
      {
        name: "latlon_to_geo",
        input_fields: [
          {
            field_name: "input_longitude",
            if_missing: "SKIP",
            is_drop: true,
            param: "longitude",
          },
          {
            field_name: "input_latitude",
            if_missing: "SKIP",
            is_drop: true,
            param: "latitude",
          },
        ],
        output_field: {
          field_name: "geography",
          on_error: "FAIL",
          value: {
            sql:
              "ST_GEOGPOINT(CAST(:longitude AS float), CAST(:latitude AS float))",
          },
        },
      },
    ];
    const createCollection = await rockset.collections.createCollection(
      "commons",
      {
        name: collectionName,
        description: "Telemetry data from Rebelle Rally",
        field_mappings: mappings,
      }
    );
    console.log("Collection Created");
  }
};

//* Use the encrypted token to build the appropriate infrastructure.
rocksetToken.apply((token) => {
  rocksetTest(token);
  const tags = {
    Owner: "ADD_OWNER_TAG",
    Customer: customer,
    UseCase: "ADD_DESCRIPTIVE_TAG",
  };

  //* Create S3 Bucket
  //* This is where the all the ingested data ends up.
  const bucket_raw = new aws.s3.Bucket(`${customer}Archive`, { tags: tags });

  //* Create Dynamo Table
  //* This manages the state and location of all assets
  const assetTable = new aws.dynamodb.Table(`${customer}AssetTable`, {
    attributes: [
      {
        name: "id",
        type: "N",
      },
    ],
    hashKey: "id",
    ttl: {
      attributeName: "expiration",
      enabled: true,
    },
    billingMode: "PAY_PER_REQUEST",
    tags: tags,
  });

  // //* Create API to read Dynamo
  // //* This will scan the table and convert it to geojson for consumption via GL-JS
  const endpoint = new awsx.apigateway.API(`${customer}Query`, {
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
  const ingestStream = new aws.kinesis.Stream(`ingest${customer}Assets`, {
    //To increase scale, increase your shard count
    shardCount: 1,
    retentionPeriod: 72,
    tags: tags,
  });

  // //* Create IoT Rule to push into Kinesis stream
  // //* This forwards all data on the specific channel into Kinesis
  const iotRole = new aws.iam.Role(`${customer}iotRole`, {
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

  const iotRolePolicy = new aws.iam.RolePolicy(`${customer}IotRolePolicy`, {
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

  const iotRule = new aws.iot.TopicRule(`${customer}IotAssetIngest`, {
    name: pulumi.interpolate`${customer}assetIngest`,
    description: "Pass from IoT Core to Asset Tracking",
    enabled: true,
    kinesis: {
      partitionKey: "id",
      roleArn: iotRole.arn,
      streamName: ingestStream.name,
    },
    //If you want to downselect from your stream, you can change this.
    sql: `SELECT * FROM 'assetingest' where id = ${identifier}`,
    sqlVersion: "2015-10-08",
  });

  // //* Create Firehose and associated IAM roles to accept your archived data
  // //* Firehose will collect and deposit enriched data into S3
  const firehoseIAMRole = new aws.iam.Role(`${customer}FirehoseRole`, {
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
    `${customer}FirehoseRolePolicy`,
    {
      role: firehoseIAMRole.id,
      policy: bucket_raw.arn.apply((arn) => {
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
    `${customer}AssetFirehose`,
    {
      destination: "extended_s3",
      extendedS3Configuration: {
        bucketArn: bucket_raw.arn,
        bufferInterval: 600,
        //This defines the size of output written to S3. Larger = bigger payload.
        bufferSize: 10,
        compressionFormat: "GZIP",
        roleArn: firehoseIAMRole.arn,
      },
    }
  );

  //* Create Lambda to process data in Kinesis
  //* This will push to Dynamo, Firehose, and Rockset.
  //* Rockset's collection mapping will set _event time to device time and create geospatial columns.
  //* This leverages two sets of indices - temporal and geospatial, for fast querying.
  const kinesisLambdaRole = new aws.iam.Role(`${customer}KinesisLambdaRole`, {
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
    `${customer}KinesisLambdaRolePolicy`,
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

  const kinesisLambda = new aws.lambda.CallbackFunction(
    `${customer}StreamProcessor`,
    {
      role: kinesisLambdaRole,
      runtime: "nodejs12.x",
      timeout: 30,
      callback: async (event, context, callback) => {
        const AWS = require("aws-sdk");
        const collectionName = `${customer}Rebelle`;
        const apiserver = "https://api.rs2.usw2.rockset.com";
        const rockset = require("rockset").default(token, apiserver);
        const ddb = new AWS.DynamoDB.DocumentClient({
          apiVersion: "2012-10-08",
        });
        const fh = new AWS.Firehose();
        const iot = new AWS.IotData({
          endpoint: IoTEndpoint,
        });
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
          let rocksetData = {};

          console.log("Building Params");
          const buildKeys = parsedKeys.filter(
            (key) => !staticParams.includes(key)
          );
          params.Item.ts = parsedData.timestamp;
          if (coordinates) {
            const elevation = await elevationQuery.queryElevation(coordinates);
            params.Item.elevation = elevation;
            rocksetData.elevation = elevation;
          }
          const longitude = coordinates !== null ? coordinates[0] : null;
          const latitude = coordinates !== null ? coordinates[1] : null;
          params.Item.longitude = latitude;
          params.Item.latitude = longitude;
          params.Item.id = parsedData.id;
          buildKeys.forEach((key) => {
            params.Item[key] = parsedData[key];
            rocksetData[key] = parsedData[key];
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

          console.log("Writing to Rockset");
          rocksetData.input_ts = new Date(parsedData.timestamp * 1000);
          rocksetData.input_longitude =
            coordinates !== null ? coordinates[0] : null;
          rocksetData.input_latitude =
            coordinates !== null ? coordinates[1] : null;
          rocksetData.id = parsedData.id;
          const rocksetStatus = await rockset.documents.addDocuments(
            "commons",
            collectionName,
            {
              data: [rocksetData],
            }
          );
          console.log(rocksetStatus);
        }
      },
      tags: tags,
    }
  );

  const kinesisLambdaEventMapping = new aws.lambda.EventSourceMapping(
    `${customer}Map2Kinesis`,
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

  // //* This exports the URL that queries Dynamo
  exports.url = endpoint.url;
});
