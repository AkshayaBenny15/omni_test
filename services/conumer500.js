const kafkaMessaging = require("../connections/kafka");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const start = async () => {
    const consumer = await kafkaMessaging.initConsumer();
    const producer = await kafkaMessaging.initProducer();
    await consumer.connect();
    console.log("Kafka Consumer and Producer initialized successfully", consumer);
    await consumer.subscribe({
        topic: "omni.192.9.200.234.envq1",
        fromBeginning: true
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                console.log(message);
                const data = JSON.parse(
                    message.value.toString()
                );

                if (
                    data.dtls?.[0]?.actn === 500 &&
                    data.dtls?.[0]?.stat === 0
                ) {
                    data.dtls[0].stat = 1;

                    await producer.send({
                        topic,
                        messages: [
                            {
                                value: JSON.stringify(data)
                            }
                        ]
                    });

                    console.log(
                        `Republished with stat=1 | topic=${topic} | partition=${partition} | offset=${message.offset}`
                    );
                }
            } catch (error) {
                console.error(
                    "Error processing message:",
                    error
                );
            }
        }
    });

    console.log("Consumer started...");
};

start().catch((error) => {
    console.error("Consumer startup failed:", error);
});