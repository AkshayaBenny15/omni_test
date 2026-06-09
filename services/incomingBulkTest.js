const kafkaMessaging = require("../connections/kafka");
const redisConnection = require("../connections/redis");

const redis = redisConnection.getClient();

async function startLoadTest(response) {
  try {
    // -----------------------------------
    // Initialize sequence key
    // -----------------------------------
    const exists = await redis.exists("omni:test:seq");

    if (!exists) {
      await redis.set("omni:test:seq", 0);
    }

    // -----------------------------------
    // Get token
    // -----------------------------------
    const token = response.data.header.tokn;

    console.log("TOKEN:", token);

    // -----------------------------------
    // Get topics
    // -----------------------------------
    const topics = Object.values(response.data.body.bsnq);

    console.log("TOPICS:", topics);

    // -----------------------------------
    // Send 1000 messages as one batch
    // -----------------------------------
    for (const topic of topics) {
      console.log(`SENDING TO TOPIC: ${topic}`);

      const messages = [];

      for (let i = 0; i < 1000; i++) {
        const seq_key = await redis.incr("omni:test:seq");

        await redis.set(`cseq:${seq_key}`, Date.now());

        const payload = {
          hdr: {
            hash: token,
            mtyp: 10,
            cseq: seq_key,
            call: ""
          },
          dtls: [
            {
              actn: 99,
              chnl: 3,
              frnm: "+917306743590",
              tonm: "8129643877",
              rdnm: "",
              invt: {},
              dring: new Date().toISOString(),
              evnt: 1
            }
          ]
        };

        messages.push({
          key: String(seq_key),
          value: JSON.stringify(payload)
        });
      }

      await kafkaMessaging.publishMessage(
        topic,
        messages
      );

      console.log(
        `${messages.length} messages sent to ${topic}`
      );
    }

    console.log("LOAD TEST COMPLETED");
  } catch (error) {
    console.error("LOAD TEST ERROR:", error);
  }
}

module.exports = startLoadTest;