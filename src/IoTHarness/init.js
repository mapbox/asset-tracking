const awsIot = require("aws-iot-device-sdk");
const turf = require("@turf/along").default;
const route = require("./route.json");

const device = awsIot.device({
  keyPath: "./harness-private.pem.key",
  certPath: "./harness-certificate.pem.crt",
  caPath: "./AmazonRootCA1.pem",
  clientId: "testing",
  host: "$$INSERT$$"
});

device.on("connect", () => {
  console.log("connected");
  runTest();
  // * Uncomment this out to subscribe
  // * Then uncomment the message event handler below
  // device.subscribe("assetingest");
});

device.on("reconnect", () => {
  console.log("reconnected");
});

//*  Uncomment to see the feed from the stream
// device.on("message", (topic, payload) => {
//   console.log("message", topic, payload.toString());
// });

const runTest = () => {
  let start = 0.1;
  const options = { units: "kilometers" };
  setInterval(() => {
    //Calculate progress along each route
    const locations = route.features.map(feature =>
      turf(feature, start, options)
    );
    //Push those positions into IoT
    locations.forEach((location, index) => {
      const coordinates = location.geometry.coordinates;
      const timestamp = Math.floor(Date.now() / 1000);
      const id = index
      device.publish(
        "assetingest",
        JSON.stringify({
          id: id,
          coordinates: coordinates,
          timestamp: timestamp
        })
      );
    });
    console.log(`Published:${Math.floor(Date.now() / 1000)}`);
    start += 0.1;
  }, 1000);
};
