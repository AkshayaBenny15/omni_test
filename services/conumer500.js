const kafkaMessaging = require("../connections/kafka");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const redisConnection = require("../connections/redis");
const SnappyCodec = require("kafkajs-snappy");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const redis = redisConnection.getClient();

const start = async () => {
    const consumer = await kafkaMessaging.initConsumer();
    const producer = await kafkaMessaging.initProducer();

    await consumer.connect();

    await consumer.subscribe({
        topic: "omni.192.9.200.234.envq1",
        fromBeginning: true
    });

    await consumer.run({
        eachBatchAutoResolve: false,

        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary
        }) => {

            try {

                console.log(
                    `Received batch of ${batch.messages.length} messages`
                );

                const outgoingMessages = [];

                for (const message of batch.messages) {

                    try {

                        const data = JSON.parse(
                            message.value.toString()
                        );

                        if (
                            data.dtls?.[0]?.actn === 500 &&
                            data.dtls?.[0]?.stat === 0
                        ) {

                            data.dtls[0].stat = 1;
                            data.dtls[0].expr =
                                new Date().toISOString();

                            const cseq = data.hdr?.cseq;

                            const sentAt = await redis.get(
                                `cseq:${cseq}`
                            );

                            if (sentAt) {

                                const latency =
                                    Date.now() - Number(sentAt);

                                console.log({
                                    cseq,
                                    latencyMs: latency
                                });
                            }

                            outgoingMessages.push({
                                value: JSON.stringify(data)
                            });
                        }

                        resolveOffset(message.offset);

                    } catch (err) {

                        console.error(
                            "Message processing error:",
                            err
                        );
                    }
                }

                if (outgoingMessages.length > 0) {

                    await producer.send({
                        topic: batch.topic,
                        messages: outgoingMessages
                    });

                    console.log(
                        `Republished ${outgoingMessages.length} messages`
                    );
                }

                await commitOffsetsIfNecessary();
                await heartbeat();

            } catch (error) {

                console.error(
                    "Batch processing error:",
                    error
                );
            }
        }
    });

    console.log("Consumer started...");
};

start().catch((error) => {
    console.error(
        "Consumer startup failed:",
        error
    );
});