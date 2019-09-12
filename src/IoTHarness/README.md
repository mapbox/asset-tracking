# IoTHarness

A node script for testing IoT Core ingest over MQTT.

## To Use

From this root folder:

```bash
npm install
node index.js
```

You can see the timestamps being sent via your terminal, and track ingestion via CloudWatch or the DynamoDB console.

### Harness Details

The initial Pulumi script will download and write the necessary certificates to run the harness. If you choose to send additional data, you can send any other data you wish. However, the pipeline requires the following three keys (and their stated types).

```javascript
JSON.stringify({
  id: 1, //Expects number
  timestamp: 1559158701, //Expects epoch timestamp
  coordinates: [-88.27841115184128, 43.05916168726981], //Expects array of floats
  ...
})
```

Any other data will automatically be ingested into Dynamo as written.
