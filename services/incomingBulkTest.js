const kafkaMessaging = require("../connections/kafka");
const redisConnection = require("../connections/redis");

const redis = redisConnection.getClient();

async function startLoadTest(response) {
  try {

    const exists = await redis.exists("omni:test:seq");

    if (!exists) {
      await redis.set("omni:test:seq", 0);
    }

    const token = response.data.header.tokn;

    console.log("TOKEN:", token);

    const topicMessages = {};

    // Total messages to generate
    for (let i = 0; i < 1000; i++) {

      const seq_key = await redis.incr(
        "omni:test:seq"
      );
      console.log("Generated cseq:", seq_key);

      await redis.set(
        `cseq:${seq_key}`,
        Date.now()
      );

      const bucket = seq_key % 10;

      const topic =
        `omni.call.${bucket}`;

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

      if (!topicMessages[topic]) {
        topicMessages[topic] = [];
      }

      topicMessages[topic].push({
        key: String(seq_key),
        value: JSON.stringify(payload)
      });
    }

    // Send grouped batches
    for (
      const [topic, messages]
      of Object.entries(topicMessages)
    ) {

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

    console.error(
      "LOAD TEST ERROR:",
      error
    );
  }
}

module.exports = startLoadTest;