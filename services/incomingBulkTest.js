const axios = require("axios");
const kafkaMessaging = require("../connections/kafka");
const redisConnection = require("../connections/redis");
const redis = redisConnection.getClient();

async function startLoadTest() {
  try {

    // -----------------------------------
    // 1. CALL restart-Ack API
    // -----------------------------------
const exists = await redis.exists("omni:test:seq");

if (!exists) {
  await redis.set("omni:test:seq", 0);
}
const seq_key = Number(
  await redis.get("omni:test:seq")
);
    const response = await axios.post(
      "http://192.9.200.31:5015/api/pstn/restart-Ack",
      {
        header: {
          mtyp: 20,
          mfrm: 0,
          ip: "192.9.200.234",
          vers: "1.0.0.0",
          strt: "2026-03-30T10:10:30.001Z",
          actt: 0,
          tchs: 95,
        },
      }
    );

    console.log("API RESPONSE RECEIVED");

    // -----------------------------------
    // 2. GET TOKEN
    // -----------------------------------

    const token = response.data.header.tokn;

    console.log("TOKEN:", token);

    // -----------------------------------
    // 3. GET TOPICS
    // -----------------------------------

    const topics = Object.values(response.data.body.bsnq);

    console.log("TOPICS:", topics);

    // -----------------------------------
    // 4. SEND TO EACH TOPIC
    // -----------------------------------

  for (const topic of topics) {

  console.log(`SENDING TO TOPIC: ${topic}`);

  const publishPromises = [];

  for (let i = 0; i < 1000; i++) {

    publishPromises.push(
      (async () => {

        // -----------------------------------
        // UNIQUE SEQUENCE
        // -----------------------------------

        const seq_key = await redis.incr("omni:test:seq");

        // -----------------------------------
        // PAYLOAD
        // -----------------------------------

        const payload = {
          hdr: {
            hash: token,

            mtyp: 10,

            cseq: seq_key,

            call: `CALL_${topic}_${Date.now()}_${i}`,
          },

          dtls: [
            {
              actn: 99,
              chnl: 3,
              frnm: "8129643877",
              tonm: "+917306743590",
              rdnm: "",
              invt: {},
              dring: new Date().toISOString(),
              evnt: 1,
            },
          ],
        };

        // -----------------------------------
        // PUBLISH
        // -----------------------------------

        return kafkaMessaging.publishMessage(
          topic,
          payload,
          payload.hdr.call
        );

      })()
    );
  }

  // -----------------------------------
  // WAIT FOR ALL 1000
  // -----------------------------------

  await Promise.all(publishPromises);

  console.log(`1000 MESSAGES SENT TO ${topic}`);
}
    console.log("LOAD TEST COMPLETED");

  } catch (error) {
    console.error("LOAD TEST ERROR:", error);
  }
}


// startLoadTest();
module.exports = startLoadTest;