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
const seq_key = await redis.incrby(
        "omni:test:seq",1000
      );
    // Total messages to generate
    for (let i = seq_key - 1000 ; i < seq_key ; i++) {

     
      // console.log("Generated cseq:", seq_key);

      // await redis.set(
      //   `cseq:${seq_key}`,
      //   Date.now()
      // );

      const bucket = i % 10;

      const topic =
        `omni.call.${bucket}`;

      const payload = {
        hdr: {
          hash: token,
          mtyp: 10,
          cseq: i,
          call: ""
        },
        dtls: [
          {
            actn: 99,
            chnl: 3,
            frnm: "+917306743590",
            tonm: "3333333333",
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
        key: String(i),
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